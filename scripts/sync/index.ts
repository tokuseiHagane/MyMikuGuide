import "dotenv/config";
import cliProgress from "cli-progress";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { brotliCompressSync, brotliDecompressSync, constants as zlibConstants } from "node:zlib";
import { readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { hashJson, stableStringify } from "../../src/lib/json";
import { detailShellPath } from "../../src/lib/entity-paths";
import {
  albumDetailSchema,
  albumSchema,
  artistDetailSchema,
  artistSchema,
  catalogScanStateSchema,
  entityRouteManifestSchema,
  fullSyncStateSchema,
  graphSummarySchema,
  lastRunSchema,
  seedsSchema,
  songDetailSchema,
  songSchema,
  type AlbumDetail,
  type Album,
  type ArtistDetail,
  type CatalogScanState,
  type EntityRouteManifest,
  type EntitySummary,
  type Artist,
  type EntityType,
  type ExternalLink,
  type FullSyncState,
  type LastRun,
  type Seeds,
  type SongDetail,
  type Song,
} from "../../src/lib/models";
import { slugify } from "../../src/lib/slug";
import {
  closeVocadbNetworking,
  configureVocadbNetworking,
  fetchAlbumById,
  fetchAlbumMetaById,
  fetchAlbumIdsByArtistId,
  fetchArtistById,
  fetchCatalogPage,
  fetchArtistMetaById,
  fetchEntityTotalCount,
  fetchSongById,
  fetchSongMetaById,
  fetchSongIdsByArtistId,
  type QueueItem,
  type SyncMode,
} from "../../src/lib/vocadb";
import {
  createFileDerivedExporter,
  dbRoot,
  derivedRoot,
  ensureDir,
  listLegacyNormalizedEntities,
  loadLegacyFailedEntities,
  loadLegacyLastRun,
  loadSeedsFile,
  rawRoot,
  readJsonFile,
} from "./file-store";
import { SqliteSyncStore } from "./sqlite-store";
import type { CatalogManifestUpsertRecord, DerivedExporter, FailedEntityRecord, LegacyStoreSnapshot, ReconcileState } from "./storage";

const args = process.argv.slice(2);
type CliMode = SyncMode | "derive";
const mode = (args[0] ?? "incremental") as CliMode;
const dryRun = args.includes("--dry-run");

type Stats = Omit<LastRun, "startedAt" | "finishedAt">;
type ChangedEntityRecord = {
  entityType: EntityType;
  entityId: string;
  displayName: string;
  previousDisplayName: string | null;
  slug: string;
  previousSlug: string | null;
  isNew: boolean;
};
type RawEnvelope = {
  format: "br";
  fetchedAt: string;
  entityType: EntityType;
  vocadbId: number;
  version: number | null;
  payload: Record<string, unknown>;
};
type EffectiveSeeds = Omit<Seeds, "modes">;
type GlobalTotals = Record<EntityType, number>;
type PartialGlobalTotals = Partial<Record<EntityType, number>>;
type StageMetricName = "catalogPage" | "knownVersionLookup" | "probe" | "fullFetch" | "persist";
type StageMetric = {
  count: number;
  totalMs: number;
};

const stats: Stats = {
  mode,
  dryRun,
  probeRequests: 0,
  probeHits: 0,
  fullFetches: 0,
  requestCount: 0,
  rawUpdated: 0,
  normalizedUpdated: 0,
  derivedUpdated: 0,
  newEntities: 0,
  limitedEntities: 0,
  catalogPagesFetched: 0,
  catalogUniqueIdsSeen: 0,
  catalogDuplicateIdsSeen: 0,
  catalogPassesCompleted: 0,
  errors: [],
};
const fatalErrors: string[] = [];
let runtimeConfig: EffectiveSeeds;
let syncStore: SqliteSyncStore;
let derivedExporter: DerivedExporter;
let metaExporter: DerivedExporter;
const stageMetrics: Record<StageMetricName, StageMetric> = {
  catalogPage: { count: 0, totalMs: 0 },
  knownVersionLookup: { count: 0, totalMs: 0 },
  probe: { count: 0, totalMs: 0 },
  fullFetch: { count: 0, totalMs: 0 },
  persist: { count: 0, totalMs: 0 },
};

function timestampPrefix() {
  return `[${new Date().toISOString()}]`;
}

const SUMMARY_PAGE_SIZE = 48;
const RECONCILE_ENTITY_ORDER: EntityType[] = ["artist", "song", "album"];
const CATALOG_ENTITY_ORDER: EntityType[] = ["artist", "song", "album"];
const REFRESH_ONLY_REASONS = new Set(["incremental-refresh", "hot-entity-refresh", "reconcile-shard-refresh"]);
const FULL_HYDRATE_REASON = "full-hydrate";
const LEGACY_DERIVED_ARTIFACTS = [
  "artists-by-slug.json",
  "songs-by-slug.json",
  "albums-by-slug.json",
  "songs-by-year.json",
  "albums-by-year.json",
  "artist-to-songs.json",
  "artist-to-albums.json",
  "recently-updated.json",
  "new-entries.json",
  "graph-summary.json",
] as const;

function entityKey(entityType: EntityType, vocadbId: number) {
  return `${entityType}:${vocadbId}`;
}

function hasLimit(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function resolveFetchLimit(...limits: Array<number | null | undefined>) {
  const finiteLimits = limits.filter(hasLimit);
  if (finiteLimits.length === 0) {
    return null;
  }

  return Math.max(Math.min(...finiteLimits), 1);
}

function trackStageDuration(stage: StageMetricName, startedAtMs: number) {
  const metric = stageMetrics[stage];
  metric.count += 1;
  metric.totalMs += performance.now() - startedAtMs;
}

function formatStageMetrics() {
  return [
    `list=${stageMetrics.catalogPage.count > 0 ? (stageMetrics.catalogPage.totalMs / stageMetrics.catalogPage.count).toFixed(0) : "-"}ms`,
    `lookup=${stageMetrics.knownVersionLookup.count > 0 ? (stageMetrics.knownVersionLookup.totalMs / stageMetrics.knownVersionLookup.count).toFixed(0) : "-"}ms`,
    `probe=${stageMetrics.probe.count > 0 ? (stageMetrics.probe.totalMs / stageMetrics.probe.count).toFixed(0) : "-"}ms`,
    `fetch=${stageMetrics.fullFetch.count > 0 ? (stageMetrics.fullFetch.totalMs / stageMetrics.fullFetch.count).toFixed(0) : "-"}ms`,
    `persist=${stageMetrics.persist.count > 0 ? (stageMetrics.persist.totalMs / stageMetrics.persist.count).toFixed(0) : "-"}ms`,
  ].join(" ");
}

function parseNumberEnv(name: string) {
  const value = process.env[name];
  if (value == null || value.trim() === "") {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Environment variable ${name} must be a finite number`);
  }

  return parsed;
}

function parseIntegerEnv(name: string) {
  const parsed = parseNumberEnv(name);
  if (parsed == null) {
    return undefined;
  }

  if (!Number.isInteger(parsed)) {
    throw new Error(`Environment variable ${name} must be an integer`);
  }

  return parsed;
}

function parseBooleanEnv(name: string) {
  const value = process.env[name];
  if (value == null || value.trim() === "") {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(`Environment variable ${name} must be a boolean-like value`);
}

function parseNullableLimitEnv(name: string) {
  const value = process.env[name];
  if (value == null || value.trim() === "") {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  if (["null", "none", "unlimited", "off"].includes(normalized)) {
    return null;
  }

  return parseIntegerEnv(name);
}

function createProgressReporter(enabled: boolean, intervalMs: number) {
  let processedCount = 0;
  let activeCount = 0;
  let knownTotals: PartialGlobalTotals | null = null;
  let fullSyncState: {
    phase: FullSyncState["phase"];
    entityType: EntityType;
    pass: number;
    nextStart: number;
    pageSize: number;
    pagesFetched: number;
    uniqueIdsSeen: number;
    duplicateIdsSeen: number;
    hydratedCount: number;
    pendingHydrates: number | null;
  } | null = null;
  let nextSnapshotAt = Date.now() + intervalMs;
  let startedAt = Date.now();
  let lastSnapshotAt = startedAt;
  let lastSnapshotProcessed = 0;
  const useProgressBar = enabled && Boolean(process.stdout.isTTY) && process.env.GITHUB_ACTIONS !== "true";
  const progressBar = useProgressBar
    ? new cliProgress.SingleBar(
        {
          format:
            "sync [{bar}] {percentage}% | phase {phase} {entity} p{pass} cursor {cursor} | processed {value}/{total} | queued {queued} | active {active} | req {requests} | changed {changed} | errs {errors}",
          barCompleteChar: "\u2588",
          barIncompleteChar: "\u2591",
          hideCursor: true,
          clearOnComplete: false,
        },
        cliProgress.Presets.shades_classic,
      )
    : null;

  function discoveredTotal(seen: Set<string>, queued: Set<string>) {
    return Math.max(seen.size + queued.size, processedCount + activeCount, 1);
  }

  function inferScanBaseline(state: NonNullable<typeof fullSyncState>) {
    const inferredLocalPages = Math.max(Math.floor(state.nextStart / Math.max(state.pageSize, 1)), 0);
    const inferredLocalUnique = Math.max(state.nextStart, 0);
    return {
      phase: state.phase,
      entityType: state.entityType,
      pass: state.pass,
      pagesFetched: Math.max(state.pagesFetched - inferredLocalPages, 0),
      uniqueIdsSeen: Math.max(state.uniqueIdsSeen - inferredLocalUnique, 0),
      duplicateIdsSeen: state.duplicateIdsSeen,
    };
  }

  function currentFullScanMetrics() {
    if (!fullSyncState || fullSyncState.phase !== "scan") {
      return null;
    }

    const baseline = inferScanBaseline(fullSyncState);

    return {
      pagesFetched: Math.max(fullSyncState.pagesFetched - baseline.pagesFetched, 0),
      uniqueIdsSeen: Math.max(fullSyncState.uniqueIdsSeen - baseline.uniqueIdsSeen, 0),
      duplicateIdsSeen: Math.max(fullSyncState.duplicateIdsSeen - baseline.duplicateIdsSeen, 0),
    };
  }

  function fullModeProgress() {
    if (!fullSyncState) {
      return null;
    }

    if (fullSyncState.phase === "scan") {
      const scanMetrics = currentFullScanMetrics();
      const totalForEntity = knownTotals?.[fullSyncState.entityType] ?? null;
      const uniqueIdsSeen = scanMetrics?.uniqueIdsSeen ?? fullSyncState.uniqueIdsSeen;
      const total = Math.max(totalForEntity ?? uniqueIdsSeen + fullSyncState.pageSize, 1);
      const value = Math.min(uniqueIdsSeen, total);
      return { total, value };
    }

    const pendingHydrates = fullSyncState.pendingHydrates ?? 0;
    const total = Math.max(fullSyncState.hydratedCount + pendingHydrates, 1);
    const value = Math.min(fullSyncState.hydratedCount, total);
    return { total, value };
  }

  function currentProgress(seen: Set<string>, queued: Set<string>) {
    if (mode === "full") {
      return fullModeProgress() ?? { total: 1, value: 0 };
    }

    return {
      total: discoveredTotal(seen, queued),
      value: processedCount,
    };
  }

  function formatLoggedProgressBar(progress: { total: number; value: number }, width = 24) {
    const safeTotal = Math.max(progress.total, 1);
    const ratio = Math.max(0, Math.min(progress.value / safeTotal, 1));
    const filled = Math.round(ratio * width);
    const bar = `${"#".repeat(filled)}${"-".repeat(Math.max(width - filled, 0))}`;
    return ` | progress [${bar}] ${(ratio * 100).toFixed(1)}%`;
  }

  function payload(queued: Set<string>) {
    return {
      queued: queued.size,
      active: activeCount,
      requests: stats.requestCount,
      changed: stats.normalizedUpdated,
      errors: stats.errors.length,
      phase: fullSyncState?.phase ?? "-",
      entity: fullSyncState?.entityType ?? "-",
      pass: fullSyncState?.pass ?? "-",
      cursor: fullSyncState ? `${fullSyncState.nextStart}` : "-",
    };
  }

  function timestampPrefix() {
    return `[${new Date().toISOString()}]`;
  }

  function formatTotalsSuffix() {
    if (!knownTotals) {
      return "";
    }

    return ` | vocadb totals A/S/Al ${knownTotals.artist?.toLocaleString("ru-RU") ?? "?"}/${knownTotals.song?.toLocaleString("ru-RU") ?? "?"}/${knownTotals.album?.toLocaleString("ru-RU") ?? "?"}`;
  }

  function formatFullSyncSuffix() {
    if (!fullSyncState) {
      return "";
    }

    const pending =
      typeof fullSyncState.pendingHydrates === "number" ? fullSyncState.pendingHydrates.toLocaleString("ru-RU") : "?";
    const scanMetrics = currentFullScanMetrics();
    const pagesFetched = scanMetrics?.pagesFetched ?? fullSyncState.pagesFetched;
    const uniqueIdsSeen = scanMetrics?.uniqueIdsSeen ?? fullSyncState.uniqueIdsSeen;
    const duplicateIdsSeen = scanMetrics?.duplicateIdsSeen ?? fullSyncState.duplicateIdsSeen;
    return ` | full phase=${fullSyncState.phase} entity=${fullSyncState.entityType} pass=${fullSyncState.pass} start=${fullSyncState.nextStart.toLocaleString("ru-RU")} pageSize=${fullSyncState.pageSize} pages=${pagesFetched.toLocaleString("ru-RU")} unique=${uniqueIdsSeen.toLocaleString("ru-RU")} dup=${duplicateIdsSeen.toLocaleString("ru-RU")} hydrated=${fullSyncState.hydratedCount.toLocaleString("ru-RU")} pending=${pending}`;
  }

  function logFullSyncSnapshot(force = false) {
    if (!fullSyncState) {
      return;
    }

    const now = Date.now();
    if (!force && now < nextSnapshotAt) {
      return;
    }

    nextSnapshotAt = now + intervalMs;
    console.log(
      `${timestampPrefix()} full sync status${formatFullSyncSuffix()}${formatLoggedProgressBar(currentProgress(new Set(), new Set()))}${formatTotalsSuffix()}`,
    );
  }

  return {
    setGlobalTotals(totals: PartialGlobalTotals | null) {
      knownTotals = totals;
      if (!totals || Object.keys(totals).length === 0) {
        console.log(`${timestampPrefix()} Sync scope: VocaDB totalCount недоступен, продолжаю без глобальной оценки.`);
        return;
      }

      console.log(
        `${timestampPrefix()} VocaDB totals: artists=${totals.artist?.toLocaleString("ru-RU") ?? "?"}, songs=${totals.song?.toLocaleString("ru-RU") ?? "?"}, albums=${totals.album?.toLocaleString("ru-RU") ?? "?"}`,
      );
    },
    start(seen: Set<string>, queued: Set<string>) {
      startedAt = Date.now();
      lastSnapshotAt = startedAt;
      lastSnapshotProcessed = 0;
      nextSnapshotAt = startedAt + intervalMs;
      if (progressBar) {
        const progress = currentProgress(seen, queued);
        progressBar.start(progress.total, progress.value, payload(queued));
      }
    },
    setActive(count: number, seen: Set<string>, queued: Set<string>) {
      activeCount = count;
      if (progressBar) {
        const progress = currentProgress(seen, queued);
        progressBar.setTotal(progress.total);
        progressBar.update(progress.value, payload(queued));
      }
    },
    batchCompleted(size: number, seen: Set<string>, queued: Set<string>) {
      processedCount += size;
      activeCount = 0;

      if (progressBar) {
        const progress = currentProgress(seen, queued);
        progressBar.setTotal(progress.total);
        progressBar.update(progress.value, payload(queued));
        return;
      }

      const now = Date.now();
      if (now < nextSnapshotAt) {
        return;
      }

      nextSnapshotAt = now + intervalMs;
      const intervalProcessed = processedCount - lastSnapshotProcessed;
      const intervalMinutes = Math.max((now - lastSnapshotAt) / 60000, 1 / 60000);
      const totalMinutes = Math.max((now - startedAt) / 60000, 1 / 60000);
      const intervalRate = intervalProcessed / intervalMinutes;
      const averageRate = processedCount / totalMinutes;
      const totalsSuffix = formatTotalsSuffix();
      const stageSuffix = formatStageMetrics();
      const fullSyncSuffix = formatFullSyncSuffix();
      const progressBarSuffix = formatLoggedProgressBar(currentProgress(seen, queued));
      console.log(
        `${timestampPrefix()} sync progress | processed=${processedCount} discovered=${discoveredTotal(seen, queued)} queued=${queued.size} requests=${stats.requestCount} changed=${stats.normalizedUpdated} errors=${stats.errors.length} rate_now=${intervalRate.toFixed(1)}/min rate_avg=${averageRate.toFixed(1)}/min ${stageSuffix}${fullSyncSuffix}${progressBarSuffix}${totalsSuffix}`,
      );
      lastSnapshotAt = now;
      lastSnapshotProcessed = processedCount;
    },
    setFullSyncState(state: FullSyncState | null, pendingHydrates?: number | null) {
      const previous = fullSyncState;
      fullSyncState = state
        ? {
            phase: state.phase,
            entityType: state.entityType,
            pass: state.pass,
            nextStart: state.nextStart,
            pageSize: state.pageSize,
            pagesFetched: state.pagesFetched,
            uniqueIdsSeen: state.uniqueIdsSeen,
            duplicateIdsSeen: state.duplicateIdsSeen,
            hydratedCount: state.hydratedCount,
            pendingHydrates: pendingHydrates ?? null,
          }
        : null;
      if (progressBar) {
        const progress = currentProgress(new Set(), new Set());
        progressBar.setTotal(progress.total);
        progressBar.update(progress.value, payload(new Set()));
      }
      const phaseChanged =
        previous?.phase !== fullSyncState?.phase ||
        previous?.entityType !== fullSyncState?.entityType ||
        previous?.pass !== fullSyncState?.pass ||
        previous?.pendingHydrates !== fullSyncState?.pendingHydrates;
      logFullSyncSnapshot(phaseChanged);
    },
    tickFullSyncState() {
      if (progressBar) {
        const progress = currentProgress(new Set(), new Set());
        progressBar.setTotal(progress.total);
        progressBar.update(progress.value, payload(new Set()));
      }
      logFullSyncSnapshot(false);
    },
    stop(seen: Set<string>, queued: Set<string>) {
      if (progressBar) {
        const progress = currentProgress(seen, queued);
        progressBar.setTotal(progress.total);
        progressBar.update(progress.value, payload(queued));
        progressBar.stop();
      }
    },
  };
}

function applyEnvOverrides<T extends EffectiveSeeds | Seeds>(seeds: T): T {
  const envOverrides = {
    maxDepth: parseIntegerEnv("SYNC_MAX_DEPTH"),
    maxNewEntitiesPerRun: parseNullableLimitEnv("SYNC_MAX_NEW_ENTITIES_PER_RUN"),
    concurrency: parseIntegerEnv("SYNC_CONCURRENCY"),
    catalogScanConcurrency: parseIntegerEnv("SYNC_CATALOG_SCAN_CONCURRENCY"),
    probeTimeoutMs: parseIntegerEnv("SYNC_PROBE_TIMEOUT_MS"),
    fetchTimeoutMs: parseIntegerEnv("SYNC_FETCH_TIMEOUT_MS"),
    idLookupTimeoutMs: parseIntegerEnv("SYNC_ID_LOOKUP_TIMEOUT_MS"),
    rootArtistDiscographyLimit: parseNullableLimitEnv("SYNC_ROOT_ARTIST_DISCOGRAPHY_LIMIT"),
    relatedArtistDiscographyLimit: parseNullableLimitEnv("SYNC_RELATED_ARTIST_DISCOGRAPHY_LIMIT"),
    hotEntityLimit: parseNullableLimitEnv("SYNC_HOT_ENTITY_LIMIT"),
    reconcileShardSize: parseIntegerEnv("SYNC_RECONCILE_SHARD_SIZE"),
    reconcileBucketCount: parseIntegerEnv("SYNC_RECONCILE_BUCKET_COUNT"),
    catalogPageSize: parseIntegerEnv("SYNC_CATALOG_PAGE_SIZE"),
    catalogValidationPasses: parseIntegerEnv("SYNC_CATALOG_VALIDATION_PASSES"),
    retryFailedOnIncremental: parseBooleanEnv("SYNC_RETRY_FAILED_ON_INCREMENTAL"),
    storeRaw: parseBooleanEnv("SYNC_STORE_RAW"),
  };

  return {
    ...seeds,
    ...Object.fromEntries(Object.entries(envOverrides).filter(([, value]) => value !== undefined)),
  } as T;
}

function rawFilePath(entityType: EntityType, vocadbId: number) {
  return path.join(rawRoot, `${entityType}s`, `${vocadbId}.json.br`);
}

function legacyRawFilePath(entityType: EntityType, vocadbId: number) {
  return path.join(rawRoot, `${entityType}s`, `${vocadbId}.json`);
}

function normalizedId(entityType: EntityType, vocadbId: number) {
  return `${entityType}-${vocadbId}`;
}

function sourceUrl(entityType: EntityType, vocadbId: number) {
  const prefix = entityType === "artist" ? "Ar" : entityType === "song" ? "S" : "Al";
  return `https://vocadb.net/${prefix}/${vocadbId}`;
}

function detailUrl(entityType: EntityType, slug: string) {
  return detailShellPath(entityType, slug);
}

function shortDescription(value: unknown) {
  if (typeof value !== "string") {
    return "";
  }

  return value.split(/\n\s*\n/)[0]?.trim().slice(0, 280) ?? "";
}

function stringArrayFromNames(names: unknown) {
  if (!Array.isArray(names)) {
    return [];
  }

  return names
    .map((item) => (item && typeof item === "object" ? (item as { value?: unknown }).value : undefined))
    .filter((value): value is string => typeof value === "string")
    .filter((value, index, array) => array.indexOf(value) === index);
}

function normalizeExternalUrl(value: string) {
  const trimmed = value.trim().replace(/[\u200B-\u200D\u200E\u200F\uFEFF]/g, "");
  if (!trimmed) {
    return null;
  }

  const sanitized = trimmed.replace(/^\/+(https?:\/\/)/i, "$1");
  const candidates = [sanitized];

  if (sanitized.startsWith("//")) {
    candidates.push(`https:${sanitized}`);
  } else if (!URL.canParse(sanitized)) {
    candidates.push(`https://${sanitized}`);
  }

  for (const candidate of candidates) {
    if (!URL.canParse(candidate)) {
      continue;
    }

    return new URL(candidate);
  }

  return null;
}

function buildExternalLink(
  urlValue: string,
  options: {
    label?: string;
    kind: string;
    service?: string;
    official: boolean;
  },
): ExternalLink | null {
  const parsedUrl = normalizeExternalUrl(urlValue);
  if (!parsedUrl) {
    return null;
  }

  const hostname = parsedUrl.hostname.replace(/^www\./, "");
  return {
    url: parsedUrl.href,
    label: options.label || hostname || parsedUrl.href,
    kind: options.kind,
    service: options.service || hostname || parsedUrl.protocol.replace(/:$/, "") || "external",
    official: options.official,
  } satisfies ExternalLink;
}

function mapWebLinks(webLinks: unknown): ExternalLink[] {
  if (!Array.isArray(webLinks)) {
    return [];
  }

  return webLinks
    .map((item) => {
      if (!item || typeof item !== "object") {
        return null;
      }

      const link = item as Record<string, unknown>;
      const url = typeof link.url === "string" ? link.url : null;
      if (!url) {
        return null;
      }

      const description = typeof link.description === "string" ? link.description : "";
      const category = typeof link.category === "string" ? link.category.toLowerCase() : "external";

      return buildExternalLink(url, {
        label: description,
        kind: category,
        official: Boolean(link.disabled === false || link.disabled === undefined),
      });
    })
    .filter((item): item is ExternalLink => item !== null);
}

function mapPvs(pvs: unknown): ExternalLink[] {
  if (!Array.isArray(pvs)) {
    return [];
  }

  return pvs
    .map((item) => {
      if (!item || typeof item !== "object") {
        return null;
      }

      const pv = item as Record<string, unknown>;
      const url = typeof pv.url === "string" ? pv.url : null;
      if (!url) {
        return null;
      }

      return buildExternalLink(url, {
        label: typeof pv.name === "string" && pv.name ? pv.name : String(pv.service ?? "PV"),
        kind: typeof pv.pvType === "string" ? pv.pvType.toLowerCase() : "pv",
        service: typeof pv.service === "string" ? pv.service.toLowerCase() : "external",
        official: pv.pvType === "Original",
      });
    })
    .filter((item): item is ExternalLink => item !== null);
}

function releaseDateToIso(releaseDate: unknown) {
  if (typeof releaseDate === "string") {
    return releaseDate;
  }

  if (releaseDate && typeof releaseDate === "object") {
    const data = releaseDate as { year?: unknown; month?: unknown; day?: unknown; isEmpty?: unknown };
    if (data.isEmpty === true || typeof data.year !== "number") {
      return null;
    }

    const month = typeof data.month === "number" && data.month > 0 ? data.month : 1;
    const day = typeof data.day === "number" && data.day > 0 ? data.day : 1;
    return new Date(Date.UTC(data.year, month - 1, day)).toISOString();
  }

  return null;
}

async function readCompressedJsonFile<T>(targetPath: string) {
  try {
    const compressed = await readFile(targetPath);
    const json = brotliDecompressSync(compressed).toString("utf8");
    return JSON.parse(json) as T;
  } catch {
    return null;
  }
}

async function deleteLegacyRawFile(targetPath: string) {
  try {
    await rm(targetPath);
  } catch {
    // Ignore missing legacy files.
  }
}

function extractVersion(raw: Record<string, unknown>) {
  return typeof raw.version === "number" ? raw.version : null;
}

async function readStoredRaw(entityType: EntityType, vocadbId: number) {
  const compressedPath = rawFilePath(entityType, vocadbId);
  const compressed = await readCompressedJsonFile<RawEnvelope>(compressedPath);
  if (compressed?.payload) {
    return {
      path: compressedPath,
      payload: compressed.payload,
      version: compressed.version,
      isCompressed: true,
    };
  }

  const legacyPath = legacyRawFilePath(entityType, vocadbId);
  const legacy = await readJsonFile<Record<string, unknown> | null>(legacyPath, null);
  if (legacy) {
    return {
      path: legacyPath,
      payload: legacy,
      version: extractVersion(legacy),
      isCompressed: false,
    };
  }

  return null;
}

async function writeCompressedRawIfChanged(
  entityType: EntityType,
  vocadbId: number,
  raw: Record<string, unknown>,
  fetchedAt: string,
) {
  const targetPath = rawFilePath(entityType, vocadbId);
  const envelope: RawEnvelope = {
    format: "br",
    fetchedAt,
    entityType,
    vocadbId,
    version: extractVersion(raw),
    payload: raw,
  };
  const nextContent = stableStringify(envelope);

  const previousEnvelope = await readCompressedJsonFile<RawEnvelope>(targetPath);
  if (previousEnvelope) {
    const previousContent = stableStringify(previousEnvelope);
    if (previousContent === nextContent) {
      return false;
    }
  }

  if (!dryRun) {
    const nextCompressed = brotliCompressSync(Buffer.from(nextContent, "utf8"), {
      params: {
        [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
      },
    });
    await ensureDir(targetPath);
    await writeFile(targetPath, nextCompressed);
    await deleteLegacyRawFile(legacyRawFilePath(entityType, vocadbId));
  }

  stats.rawUpdated += 1;
  return true;
}

async function removeRawArtifact(entityType: EntityType, vocadbId: number) {
  if (dryRun) {
    return;
  }

  await Promise.all([
    rm(rawFilePath(entityType, vocadbId), { force: true }),
    rm(legacyRawFilePath(entityType, vocadbId), { force: true }),
  ]);
}

async function clearRawEntityCaches() {
  if (dryRun) {
    return;
  }

  await Promise.all([
    rm(path.join(rawRoot, "artists"), { recursive: true, force: true }),
    rm(path.join(rawRoot, "songs"), { recursive: true, force: true }),
    rm(path.join(rawRoot, "albums"), { recursive: true, force: true }),
  ]);
}

function resolveSeedsForMode(seeds: Seeds, currentMode: SyncMode): EffectiveSeeds {
  const overrides = seeds.modes[currentMode] ?? {};
  return {
    ...seeds,
    ...overrides,
  };
}

async function loadSeeds() {
  const parsedSeeds = seedsSchema.parse(await loadSeedsFile());
  const seedMode: SyncMode = mode === "derive" ? "full" : mode;
  return applyEnvOverrides(resolveSeedsForMode(parsedSeeds, seedMode));
}

function buildArtist(raw: Record<string, unknown>, syncedAt: string) {
  const names = stringArrayFromNames(raw.names);
  const displayName =
    (typeof raw.name === "string" && raw.name) ||
    (typeof raw.defaultName === "string" && raw.defaultName) ||
    names[0] ||
    `artist-${raw.id}`;
  const vocadbId = Number(raw.id);
  const slug = slugify(displayName, vocadbId);
  const artistLinks = Array.isArray(raw.artistLinks) ? raw.artistLinks : [];

  const artist = {
    id: normalizedId("artist", vocadbId),
    entityType: "artist",
    vocadbId,
    slug,
    url: detailUrl("artist", slug),
    name: displayName,
    defaultName: typeof raw.defaultName === "string" ? raw.defaultName : displayName,
    displayName,
    additionalNames: names.filter((value) => value !== displayName),
    artistType: typeof raw.artistType === "string" ? raw.artistType : "Unknown",
    descriptionShort: shortDescription(raw.description),
    primaryImage:
      raw.mainPicture && typeof raw.mainPicture === "object" && typeof (raw.mainPicture as { urlOriginal?: unknown }).urlOriginal === "string"
        ? ((raw.mainPicture as { urlOriginal: string }).urlOriginal ?? null)
        : null,
    groups: artistLinks
      .filter((link) => link && typeof link === "object" && (link as { linkType?: unknown }).linkType === "Group")
      .map((link) => normalizedId("artist", Number((link as { artist?: { id?: unknown } }).artist?.id)))
      .filter(Boolean),
    voicebanks: artistLinks
      .filter((link) => link && typeof link === "object" && (link as { linkType?: unknown }).linkType === "VoiceProvider")
      .map((link) => normalizedId("artist", Number((link as { artist?: { id?: unknown } }).artist?.id)))
      .filter(Boolean),
    songIds: [],
    albumIds: [],
    relatedArtistIds: artistLinks
      .map((link) => normalizedId("artist", Number((link as { artist?: { id?: unknown } }).artist?.id)))
      .filter(Boolean),
    externalLinks: mapWebLinks(raw.webLinks),
    source: "vocadb",
    sourceUrl: sourceUrl("artist", vocadbId),
    syncedAt,
    upstreamVersion: extractVersion(raw),
    upstreamUpdatedAt: null,
    rawHash: hashJson(raw),
    status: "active",
  } satisfies Artist;

  return artistSchema.parse(artist);
}

function buildSong(raw: Record<string, unknown>, syncedAt: string) {
  const names = stringArrayFromNames(raw.names);
  const displayName =
    (typeof raw.name === "string" && raw.name) ||
    (typeof raw.defaultName === "string" && raw.defaultName) ||
    names[0] ||
    `song-${raw.id}`;
  const vocadbId = Number(raw.id);
  const slug = slugify(displayName, vocadbId);
  const artists = Array.isArray(raw.artists) ? raw.artists : [];
  const albums = Array.isArray(raw.albums) ? raw.albums : [];
  const tags = Array.isArray(raw.tags) ? raw.tags : [];

  const song = {
    id: normalizedId("song", vocadbId),
    entityType: "song",
    vocadbId,
    slug,
    url: detailUrl("song", slug),
    title: displayName,
    defaultName: typeof raw.defaultName === "string" ? raw.defaultName : displayName,
    displayName,
    additionalNames: names.filter((value) => value !== displayName),
    artistIds: artists
      .map((artist) => normalizedId("artist", Number((artist as { artist?: { id?: unknown } }).artist?.id)))
      .filter(Boolean),
    albumIds: albums.map((album) => normalizedId("album", Number((album as { id?: unknown }).id))).filter(Boolean),
    vocalistIds: artists
      .filter((artist) => {
        const categories = (artist as { categories?: unknown }).categories;
        return typeof categories === "string" && categories.includes("Vocalist");
      })
      .map((artist) => normalizedId("artist", Number((artist as { artist?: { id?: unknown } }).artist?.id)))
      .filter(Boolean),
    year:
      typeof raw.publishDate === "string" && raw.publishDate
        ? new Date(raw.publishDate).getUTCFullYear()
        : null,
    publishDate: typeof raw.publishDate === "string" ? raw.publishDate : null,
    durationSeconds: typeof raw.lengthSeconds === "number" ? raw.lengthSeconds : null,
    songType: typeof raw.songType === "string" ? raw.songType : "Unspecified",
    tags: tags
      .map((tag) => (tag && typeof tag === "object" ? (tag as { tag?: { name?: unknown } }).tag?.name : undefined))
      .filter((value): value is string => typeof value === "string"),
    primaryImage:
      raw.mainPicture && typeof raw.mainPicture === "object" && typeof (raw.mainPicture as { urlOriginal?: unknown }).urlOriginal === "string"
        ? ((raw.mainPicture as { urlOriginal: string }).urlOriginal ?? null)
        : null,
    externalLinks: [...mapWebLinks(raw.webLinks), ...mapPvs(raw.pvs)],
    source: "vocadb",
    sourceUrl: sourceUrl("song", vocadbId),
    syncedAt,
    upstreamVersion: extractVersion(raw),
    upstreamUpdatedAt: null,
    rawHash: hashJson(raw),
    status: "active",
  } satisfies Song;

  return songSchema.parse(song);
}

function buildAlbum(raw: Record<string, unknown>, syncedAt: string) {
  const names = stringArrayFromNames(raw.names);
  const displayName =
    (typeof raw.name === "string" && raw.name) ||
    (typeof raw.defaultName === "string" && raw.defaultName) ||
    names[0] ||
    `album-${raw.id}`;
  const vocadbId = Number(raw.id);
  const slug = slugify(displayName, vocadbId);
  const tracks = Array.isArray(raw.tracks) ? raw.tracks : [];
  const artists = Array.isArray(raw.artists) ? raw.artists : [];

  const album = {
    id: normalizedId("album", vocadbId),
    entityType: "album",
    vocadbId,
    slug,
    url: detailUrl("album", slug),
    title: displayName,
    defaultName: typeof raw.defaultName === "string" ? raw.defaultName : displayName,
    displayName,
    additionalNames: names.filter((value) => value !== displayName),
    artistIds: artists
      .map((artist) => normalizedId("artist", Number((artist as { artist?: { id?: unknown } }).artist?.id)))
      .filter(Boolean),
    songIds: tracks
      .map((track) => normalizedId("song", Number((track as { song?: { id?: unknown } }).song?.id)))
      .filter(Boolean),
    tracks: tracks
      .map((track) => {
        const item = track as Record<string, unknown>;
        const songId = Number((item.song as { id?: unknown } | undefined)?.id);
        if (!songId) {
          return null;
        }
        return {
          trackNumber: typeof item.trackNumber === "number" ? item.trackNumber : null,
          discNumber: typeof item.discNumber === "number" ? item.discNumber : null,
          songId: normalizedId("song", songId),
          name: typeof item.name === "string" ? item.name : `song-${songId}`,
        };
      })
      .filter((track): track is NonNullable<typeof track> => track !== null),
    year:
      releaseDateToIso(raw.releaseDate) !== null ? new Date(releaseDateToIso(raw.releaseDate)!).getUTCFullYear() : null,
    releaseDate: releaseDateToIso(raw.releaseDate),
    albumType: typeof raw.discType === "string" ? raw.discType : "Album",
    catalogNumber: typeof raw.catalogNumber === "string" ? raw.catalogNumber : null,
    primaryImage:
      raw.mainPicture && typeof raw.mainPicture === "object" && typeof (raw.mainPicture as { urlOriginal?: unknown }).urlOriginal === "string"
        ? ((raw.mainPicture as { urlOriginal: string }).urlOriginal ?? null)
        : null,
    externalLinks: mapWebLinks(raw.webLinks),
    source: "vocadb",
    sourceUrl: sourceUrl("album", vocadbId),
    syncedAt,
    upstreamVersion: extractVersion(raw),
    upstreamUpdatedAt: null,
    rawHash: hashJson(raw),
    status: "active",
  } satisfies Album;

  return albumSchema.parse(album);
}

async function fetchRaw(item: QueueItem) {
  const startedAtMs = performance.now();
  stats.fullFetches += 1;
  stats.requestCount += 1;
  try {
    if (item.entityType === "artist") {
      return await fetchArtistById(item.vocadbId, { timeoutMs: runtimeConfig.fetchTimeoutMs });
    }
    if (item.entityType === "song") {
      return await fetchSongById(item.vocadbId, { timeoutMs: runtimeConfig.fetchTimeoutMs });
    }
    return await fetchAlbumById(item.vocadbId, { timeoutMs: runtimeConfig.fetchTimeoutMs });
  } finally {
    trackStageDuration("fullFetch", startedAtMs);
  }
}

async function fetchProbeMeta(item: QueueItem) {
  const startedAtMs = performance.now();
  stats.probeRequests += 1;
  stats.requestCount += 1;
  try {
    if (item.entityType === "artist") {
      return await fetchArtistMetaById(item.vocadbId, { timeoutMs: runtimeConfig.probeTimeoutMs });
    }
    if (item.entityType === "song") {
      return await fetchSongMetaById(item.vocadbId, { timeoutMs: runtimeConfig.probeTimeoutMs });
    }
    return await fetchAlbumMetaById(item.vocadbId, { timeoutMs: runtimeConfig.probeTimeoutMs });
  } finally {
    trackStageDuration("probe", startedAtMs);
  }
}

async function getRawForItem(item: QueueItem) {
  const stored = runtimeConfig.storeRaw ? await readStoredRaw(item.entityType, item.vocadbId) : null;
  const knownVersion = stored?.version ?? item.knownVersion ?? null;

  if (knownVersion !== null && stored?.payload) {
    const probe = await fetchProbeMeta(item);
    if (typeof probe.version === "number" && probe.version === knownVersion) {
      stats.probeHits += 1;
      if (!stored.isCompressed && runtimeConfig.storeRaw) {
        await writeCompressedRawIfChanged(item.entityType, item.vocadbId, stored.payload, new Date().toISOString());
      }
      return {
        raw: stored.payload,
        changed: false,
        upstreamVersion: probe.version,
      };
    }
  }

  const raw = await fetchRaw(item);
  if (runtimeConfig.storeRaw) {
    await writeCompressedRawIfChanged(item.entityType, item.vocadbId, raw, new Date().toISOString());
  } else {
    await removeRawArtifact(item.entityType, item.vocadbId);
  }
  return {
    raw,
    changed: true,
    upstreamVersion: extractVersion(raw),
  };
}

async function normalizeAndPersist(item: QueueItem, raw: Record<string, unknown>, syncedAt: string) {
  const startedAtMs = performance.now();
  try {
  if (item.entityType === "artist") {
    const artist = buildArtist(raw, syncedAt);
    const persisted = await syncStore.persistEntity(artist);
    if (persisted.changed) {
      stats.normalizedUpdated += 1;
    }
    return {
      entity: artist,
      isNew: persisted.isNew,
      changed: persisted.changed,
      previousEntity: persisted.previousEntity,
    };
  }
  if (item.entityType === "song") {
    const song = buildSong(raw, syncedAt);
    const persisted = await syncStore.persistEntity(song);
    if (persisted.changed) {
      stats.normalizedUpdated += 1;
    }
    return {
      entity: song,
      isNew: persisted.isNew,
      changed: persisted.changed,
      previousEntity: persisted.previousEntity,
    };
  }

  const album = buildAlbum(raw, syncedAt);
  const persisted = await syncStore.persistEntity(album);
  if (persisted.changed) {
    stats.normalizedUpdated += 1;
  }
  return {
    entity: album,
    isNew: persisted.isNew,
    changed: persisted.changed,
    previousEntity: persisted.previousEntity,
  };
  } finally {
    trackStageDuration("persist", startedAtMs);
  }
}

async function writeArtifactIfChanged(
  exporter: DerivedExporter,
  relativePath: string,
  value: unknown,
  counter: "rawUpdated" | "derivedUpdated",
) {
  const updated = await exporter.writeArtifact(relativePath, value);
  if (updated) {
    stats[counter] += 1;
  }
  return updated;
}

async function deleteArtifactIfExists(exporter: DerivedExporter, relativePath: string, counter: "rawUpdated" | "derivedUpdated") {
  const removed = await exporter.deleteArtifact(relativePath);
  if (removed) {
    stats[counter] += 1;
  }
  return removed;
}

function pluralEntityType(entityType: EntityType) {
  if (entityType === "artist") {
    return "artists";
  }
  if (entityType === "song") {
    return "songs";
  }
  return "albums";
}

function summaryPagePath(entityType: EntityType, pageNumber: number) {
  return `summary/${pluralEntityType(entityType)}/page-${String(pageNumber).padStart(4, "0")}.json`;
}

function detailPath(entityType: EntityType, slug: string) {
  return `detail/${entityType}/${slug}.json`;
}

function routeManifestPath(entityType: EntityType) {
  return `meta/routes/${pluralEntityType(entityType)}.json`;
}

function buildRouteManifestFromRoutes(
  entityType: EntityType,
  items: Array<{ id: string; slug: string; displayName: string }>,
  totalItems: number,
) {
  const totalPages = Math.max(1, Math.ceil(totalItems / SUMMARY_PAGE_SIZE));
  return entityRouteManifestSchema.parse({
    entityType,
    pageSize: SUMMARY_PAGE_SIZE,
    totalItems,
    totalPages,
    items: items.map((item, index) => ({
      id: item.id,
      slug: item.slug,
      displayName: item.displayName,
      pageNumber: Math.floor(index / SUMMARY_PAGE_SIZE) + 1,
    })),
  });
}

function buildSummaryPagePayload(
  entityType: EntityType,
  items: EntitySummary[],
  pageNumber: number,
  totalItems: number,
  totalPages: number,
) {
  return entityType === "artist"
    ? {
        entityType,
        pageNumber,
        pageSize: SUMMARY_PAGE_SIZE,
        totalItems,
        totalPages,
        items,
      }
    : entityType === "song"
      ? {
          entityType,
          pageNumber,
          pageSize: SUMMARY_PAGE_SIZE,
          totalItems,
          totalPages,
          items,
        }
      : {
          entityType,
          pageNumber,
          pageSize: SUMMARY_PAGE_SIZE,
          totalItems,
          totalPages,
          items,
        };
}

function addTouchedId(target: Map<EntityType, Set<string>>, entityType: EntityType, entityId: string) {
  const current = target.get(entityType);
  if (current) {
    current.add(entityId);
    return;
  }

  target.set(entityType, new Set([entityId]));
}

async function collectTouchedDetailIds(changedEntities: ChangedEntityRecord[]) {
  const touched = new Map<EntityType, Set<string>>();
  const directArtists = new Set<string>();
  const directSongs = new Set<string>();
  const directAlbums = new Set<string>();

  for (const changed of changedEntities) {
    addTouchedId(touched, changed.entityType, changed.entityId);
    if (changed.entityType === "artist") {
      directArtists.add(changed.entityId);
    } else if (changed.entityType === "song") {
      directSongs.add(changed.entityId);
    } else {
      directAlbums.add(changed.entityId);
    }
  }

  for (const relation of syncStore.iterateArtistSongRelations()) {
    if (directArtists.has(relation.artistId)) {
      addTouchedId(touched, "song", relation.songId);
    }
    if (directSongs.has(relation.songId)) {
      addTouchedId(touched, "artist", relation.artistId);
    }
  }

  for (const relation of syncStore.iterateArtistAlbumRelations()) {
    if (directArtists.has(relation.artistId)) {
      addTouchedId(touched, "album", relation.albumId);
    }
    if (directAlbums.has(relation.albumId)) {
      addTouchedId(touched, "artist", relation.artistId);
    }
  }

  for (const relation of syncStore.iterateAlbumSongRelations()) {
    if (directAlbums.has(relation.albumId)) {
      addTouchedId(touched, "song", relation.songId);
    }
    if (directSongs.has(relation.songId)) {
      addTouchedId(touched, "album", relation.albumId);
    }
  }

  for (const relation of syncStore.iterateArtistRelations()) {
    if (directArtists.has(relation.artistId)) {
      addTouchedId(touched, "artist", relation.relatedArtistId);
    }
    if (directArtists.has(relation.relatedArtistId)) {
      addTouchedId(touched, "artist", relation.artistId);
    }
  }

  return touched;
}

function changedEntitiesByType(changedEntities: ChangedEntityRecord[]) {
  const grouped = new Map<EntityType, ChangedEntityRecord[]>();

  for (const changed of changedEntities) {
    const current = grouped.get(changed.entityType) ?? [];
    current.push(changed);
    grouped.set(changed.entityType, current);
  }

  return grouped;
}

function summaryOrderMayChange(changes: ChangedEntityRecord[]) {
  return changes.some(
    (changed) =>
      changed.isNew || changed.previousSlug !== changed.slug || changed.previousDisplayName !== changed.displayName,
  );
}

async function loadExistingRouteManifest(entityType: EntityType) {
  const manifest = await readJsonFile<EntityRouteManifest | null>(path.join(derivedRoot, routeManifestPath(entityType)), null);
  if (!manifest || manifest.entityType !== entityType) {
    return null;
  }

  return entityRouteManifestSchema.parse(manifest);
}

function summaryPagesForTouchedIdsFromManifest(manifest: EntityRouteManifest, touchedIds: Set<string>) {
  const pageNumbers = new Set<number>();

  for (const item of manifest.items) {
    if (touchedIds.has(item.id)) {
      pageNumbers.add(item.pageNumber);
    }
  }

  return [...pageNumbers].sort((left, right) => left - right);
}

async function writeSummaryPages(
  entityType: EntityType,
  pageNumbers: number[],
  totalItems: number,
  totalPages: number,
) {
  for (const pageNumber of pageNumbers) {
    const items = await syncStore.listSummaryPage(entityType, pageNumber, SUMMARY_PAGE_SIZE);
    await writeArtifactIfChanged(
      derivedExporter,
      summaryPagePath(entityType, pageNumber),
      buildSummaryPagePayload(entityType, items, pageNumber, totalItems, totalPages),
      "derivedUpdated",
    );
  }
}

function allSummaryPageNumbers(totalPages: number) {
  return Array.from({ length: totalPages }, (_, index) => index + 1);
}

async function pruneObsoleteSummaryPages(entityType: EntityType, totalPages: number) {
  const deriveMeta = syncStore.loadDeriveMeta(entityType);
  const previousTotalPages = Math.max(deriveMeta?.total_pages ?? 0, 0);

  if (previousTotalPages <= totalPages) {
    return;
  }

  for (let pageNumber = totalPages + 1; pageNumber <= previousTotalPages; pageNumber += 1) {
    await deleteArtifactIfExists(derivedExporter, summaryPagePath(entityType, pageNumber), "derivedUpdated");
  }
}

async function rebuildSummaryArtifactsForType(
  entityType: EntityType,
  totalItems: number,
  touchedIds: Set<string>,
  directChanges: ChangedEntityRecord[],
) {
  const totalPages = Math.max(1, Math.ceil(totalItems / SUMMARY_PAGE_SIZE));
  await pruneObsoleteSummaryPages(entityType, totalPages);

  const deriveMeta = syncStore.loadDeriveMeta(entityType);
  const hasReliablePreviousState =
    deriveMeta !== null &&
    deriveMeta.total_items === totalItems &&
    deriveMeta.total_pages === totalPages;
  const needsFullSummaryRebuild = hasReliablePreviousState && summaryOrderMayChange(directChanges);
  const needsRouteManifestRefresh = !hasReliablePreviousState || needsFullSummaryRebuild;

  let routeItems: Array<{ id: string; slug: string; displayName: string }> | null = null;
  let manifestForPages: EntityRouteManifest | null = null;

  if (needsRouteManifestRefresh) {
    routeItems = await syncStore.listRouteItems(entityType);
    manifestForPages = buildRouteManifestFromRoutes(entityType, routeItems, totalItems);
  }

  console.log(
    `${timestampPrefix()} derive summary | entity=${entityType} touched=${touchedIds.size.toLocaleString("ru-RU")} direct=${directChanges.length.toLocaleString("ru-RU")} total_pages=${totalPages.toLocaleString("ru-RU")} reliable_previous=${hasReliablePreviousState} route_refresh=${needsRouteManifestRefresh} full_rebuild=${needsFullSummaryRebuild}`,
  );

  if (needsRouteManifestRefresh && manifestForPages) {
    await writeArtifactIfChanged(
      derivedExporter,
      routeManifestPath(entityType),
      manifestForPages,
      "derivedUpdated",
    );
    syncStore.saveDeriveRouteState(
      entityType,
      manifestForPages.items,
      totalItems,
      totalPages,
      SUMMARY_PAGE_SIZE,
    );
  }

  if (needsFullSummaryRebuild) {
    await writeSummaryPages(entityType, allSummaryPageNumbers(totalPages), totalItems, totalPages);
    return;
  }

  if (needsRouteManifestRefresh && manifestForPages) {
    await writeSummaryPages(
      entityType,
      summaryPagesForTouchedIdsFromManifest(manifestForPages, touchedIds),
      totalItems,
      totalPages,
    );
    return;
  }

  const touchedPages = syncStore.listDerivePageNumbersForIds(entityType, touchedIds);
  await writeSummaryPages(entityType, touchedPages, totalItems, totalPages);
}

async function loadChangedEntitiesSinceLastRun() {
  const previousLastRun = await loadLegacyLastRun();
  if (!previousLastRun) {
    return [] as ChangedEntityRecord[];
  }

  console.log(`${timestampPrefix()} derive delta | since=${previousLastRun.finishedAt}`);
  const changedEntities: ChangedEntityRecord[] = [];

  for (const entityType of CATALOG_ENTITY_ORDER) {
    const currentChanges = await syncStore.listChangedEntitiesSince(entityType, previousLastRun.finishedAt);
    let previousRouteCount = 0;

    for (const current of currentChanges) {
      const previous = syncStore.loadDeriveRouteItem(entityType, current.entityId);
      if (previous) previousRouteCount += 1;
      changedEntities.push({
        entityType,
        entityId: current.entityId,
        displayName: current.displayName,
        previousDisplayName: previous?.display_name ?? null,
        slug: current.slug,
        previousSlug: previous?.slug ?? null,
        isNew: !previous,
      });
    }

    console.log(
      `${timestampPrefix()} derive delta | entity=${entityType} changed=${currentChanges.length.toLocaleString("ru-RU")} with_previous_route=${previousRouteCount.toLocaleString("ru-RU")}`,
    );
  }

  return changedEntities;
}

type DeriveStageMetric = { label: string; ms: number; count?: number };

function logDeriveMetrics(stages: DeriveStageMetric[]) {
  const parts = stages.map((s) => {
    const countSuffix = s.count != null ? ` (${s.count.toLocaleString("ru-RU")})` : "";
    return `${s.label}=${s.ms.toFixed(0)}ms${countSuffix}`;
  });
  console.log(`${timestampPrefix()} derive perf | ${parts.join(" | ")}`);
}

async function buildDerived(changedEntities: ChangedEntityRecord[]) {
  console.log(`${timestampPrefix()} derive build | changed=${changedEntities.length.toLocaleString("ru-RU")}`);
  const deriveTimings: DeriveStageMetric[] = [];
  const t0 = performance.now();
  const touchedDetailIds = await collectTouchedDetailIds(changedEntities);
  const t1 = performance.now();
  deriveTimings.push({ label: "touched_expansion", ms: t1 - t0, count: changedEntities.length });
  const touchedSummaryTypes = new Set<EntityType>([...touchedDetailIds.keys()]);
  const directChangesByType = changedEntitiesByType(changedEntities);
  console.log(
    `${timestampPrefix()} derive touched | artists=${(touchedDetailIds.get("artist")?.size ?? 0).toLocaleString("ru-RU")} songs=${(touchedDetailIds.get("song")?.size ?? 0).toLocaleString("ru-RU")} albums=${(touchedDetailIds.get("album")?.size ?? 0).toLocaleString("ru-RU")}`,
  );
  const snapshotSyncedAt = new Date().toISOString();
  const snapshotDay = snapshotSyncedAt.slice(0, 10);
  const t2 = performance.now();
  const [artistCount, songCount, albumCount, recentSummaries, updatedToday] = await Promise.all([
    syncStore.countEntities("artist"),
    syncStore.countEntities("song"),
    syncStore.countEntities("album"),
    syncStore.listRecentSummaries(250),
    syncStore.listUpdatedTodaySummaries(snapshotDay),
  ]);
  const t3 = performance.now();
  deriveTimings.push({ label: "meta_queries", ms: t3 - t2 });

  const graphSummary = graphSummarySchema.parse({
    syncedAt: snapshotSyncedAt,
    artistCount,
    songCount,
    albumCount,
    summaryPageSize: SUMMARY_PAGE_SIZE,
  });

  for (const legacyArtifact of LEGACY_DERIVED_ARTIFACTS) {
    await deleteArtifactIfExists(derivedExporter, legacyArtifact, "derivedUpdated");
  }

  const tSummaryStart = performance.now();
  let summaryPagesWritten = 0;

  if (touchedSummaryTypes.has("artist")) {
    await rebuildSummaryArtifactsForType(
      "artist",
      artistCount,
      touchedDetailIds.get("artist") ?? new Set<string>(),
      directChangesByType.get("artist") ?? [],
    );
  }

  if (touchedSummaryTypes.has("song")) {
    await rebuildSummaryArtifactsForType(
      "song",
      songCount,
      touchedDetailIds.get("song") ?? new Set<string>(),
      directChangesByType.get("song") ?? [],
    );
  }

  if (touchedSummaryTypes.has("album")) {
    await rebuildSummaryArtifactsForType(
      "album",
      albumCount,
      touchedDetailIds.get("album") ?? new Set<string>(),
      directChangesByType.get("album") ?? [],
    );
  }

  const tSummaryEnd = performance.now();
  deriveTimings.push({ label: "summary_rebuild", ms: tSummaryEnd - tSummaryStart });

  await Promise.all([
    writeArtifactIfChanged(derivedExporter, "meta/recently-updated.json", recentSummaries.slice(0, 250), "derivedUpdated"),
    writeArtifactIfChanged(derivedExporter, "meta/updated-today.json", updatedToday, "derivedUpdated"),
    writeArtifactIfChanged(derivedExporter, "meta/graph-summary.json", graphSummary, "derivedUpdated"),
  ]);

  const tDetailStart = performance.now();
  let detailsWritten = 0;
  let noOpWrites = 0;

  for (const changed of changedEntities) {
    if (changed.previousSlug && changed.previousSlug !== changed.slug) {
      await deleteArtifactIfExists(derivedExporter, detailPath(changed.entityType, changed.previousSlug), "derivedUpdated");
    }
  }

  for (const artistId of touchedDetailIds.get("artist") ?? []) {
    const artist = await syncStore.getEntityById<Artist>("artist", artistId);
    if (!artist) {
      continue;
    }

    const payload: ArtistDetail = artistDetailSchema.parse({
      entityType: "artist",
      entity: artist,
      relatedArtists: (await syncStore.listRelatedArtistSummaries(artist.id)).filter((entry) => entry.id !== artist.id),
      relatedSongs: await syncStore.listArtistSongSummaries(artist.id),
      relatedAlbums: await syncStore.listArtistAlbumSummaries(artist.id),
    });
    const written = await writeArtifactIfChanged(derivedExporter, detailPath("artist", artist.slug), payload, "derivedUpdated");
    if (written) detailsWritten += 1; else noOpWrites += 1;
  }

  for (const songId of touchedDetailIds.get("song") ?? []) {
    const song = await syncStore.getEntityById<Song>("song", songId);
    if (!song) {
      continue;
    }

    const payload: SongDetail = songDetailSchema.parse({
      entityType: "song",
      entity: song,
      relatedArtists: await syncStore.listSongArtistSummaries(song.id),
      relatedAlbums: await syncStore.listSongAlbumSummaries(song.id),
    });
    const written = await writeArtifactIfChanged(derivedExporter, detailPath("song", song.slug), payload, "derivedUpdated");
    if (written) detailsWritten += 1; else noOpWrites += 1;
  }

  for (const albumId of touchedDetailIds.get("album") ?? []) {
    const album = await syncStore.getEntityById<Album>("album", albumId);
    if (!album) {
      continue;
    }

    const payload: AlbumDetail = albumDetailSchema.parse({
      entityType: "album",
      entity: album,
      relatedArtists: await syncStore.listAlbumArtistSummaries(album.id),
      relatedSongs: await syncStore.listAlbumSongSummaries(album.id),
      tracks: await syncStore.listAlbumTracks(album.id),
    });
    const written = await writeArtifactIfChanged(derivedExporter, detailPath("album", album.slug), payload, "derivedUpdated");
    if (written) detailsWritten += 1; else noOpWrites += 1;
  }

  const tDetailEnd = performance.now();
  deriveTimings.push({ label: "detail_rebuild", ms: tDetailEnd - tDetailStart, count: detailsWritten });
  deriveTimings.push({ label: "total", ms: tDetailEnd - t0 });
  logDeriveMetrics(deriveTimings);
  console.log(
    `${timestampPrefix()} derive stats | changed=${changedEntities.length} detailsWritten=${detailsWritten} noOpWrites=${noOpWrites}`,
  );

  return {
    graphSummary,
    recentSummaries,
    updatedToday,
  };
}

async function loadLegacySnapshot(seeds: EffectiveSeeds): Promise<LegacyStoreSnapshot> {
  const [artists, songs, albums, failedEntities, lastRun] = await Promise.all([
    listLegacyNormalizedEntities<Artist>("artist"),
    listLegacyNormalizedEntities<Song>("song"),
    listLegacyNormalizedEntities<Album>("album"),
    loadLegacyFailedEntities(),
    loadLegacyLastRun(),
  ]);

  return {
    artists,
    songs,
    albums,
    failedEntities,
    lastRun,
    seeds,
  };
}

function defaultReconcileState(bucketCount: number): ReconcileState {
  return {
    entityType: "artist",
    bucket: 0,
    bucketCount,
    lastVocadbId: 0,
    cycle: 0,
    updatedAt: new Date().toISOString(),
  };
}

function normalizeReconcileState(state: ReconcileState | null, bucketCount: number): ReconcileState {
  if (!state || !RECONCILE_ENTITY_ORDER.includes(state.entityType)) {
    return defaultReconcileState(bucketCount);
  }

  const normalizedBucketCount = Math.max(bucketCount, 1);
  return {
    entityType: state.entityType,
    bucket: ((state.bucket % normalizedBucketCount) + normalizedBucketCount) % normalizedBucketCount,
    bucketCount: normalizedBucketCount,
    lastVocadbId: Math.max(state.lastVocadbId, 0),
    cycle: Math.max(state.cycle, 0),
    updatedAt: state.updatedAt,
  };
}

function nextReconcileState(current: ReconcileState, exhausted: boolean, lastVocadbId: number): ReconcileState {
  if (!exhausted) {
    return {
      ...current,
      lastVocadbId,
      updatedAt: new Date().toISOString(),
    };
  }

  const entityIndex = RECONCILE_ENTITY_ORDER.indexOf(current.entityType);
  const wrappedBucket = current.bucket + 1 >= current.bucketCount;
  const nextBucket = wrappedBucket ? 0 : current.bucket + 1;
  const nextEntityType = wrappedBucket
    ? RECONCILE_ENTITY_ORDER[(entityIndex + 1) % RECONCILE_ENTITY_ORDER.length] ?? "artist"
    : current.entityType;
  const completedCycle =
    wrappedBucket && entityIndex === RECONCILE_ENTITY_ORDER.length - 1 ? current.cycle + 1 : current.cycle;

  return {
    entityType: nextEntityType,
    bucket: nextBucket,
    bucketCount: current.bucketCount,
    lastVocadbId: 0,
    cycle: completedCycle,
    updatedAt: new Date().toISOString(),
  };
}

function defaultCatalogScanState(pageSize: number, totals: GlobalTotals | null): CatalogScanState {
  return {
    entityType: "artist",
    nextStart: 0,
    pageSize,
    pass: 1,
    initialTotals: totals,
    latestTotals: totals,
    pagesFetched: 0,
    uniqueIdsSeen: 0,
    duplicateIdsSeen: 0,
    updatedAt: new Date().toISOString(),
  };
}

function normalizeCatalogScanState(
  state: CatalogScanState | null,
  pageSize: number,
  totals: GlobalTotals | null,
): CatalogScanState {
  if (!state || !CATALOG_ENTITY_ORDER.includes(state.entityType)) {
    return defaultCatalogScanState(pageSize, totals);
  }

  return catalogScanStateSchema.parse({
    ...state,
    pageSize,
    initialTotals: state.initialTotals ?? totals,
    latestTotals: totals ?? state.latestTotals,
    updatedAt: state.updatedAt,
  });
}

function totalCatalogPasses(config: EffectiveSeeds) {
  return Math.max(config.catalogValidationPasses, 0) + 1;
}

function resolveCatalogScanConcurrency(config: EffectiveSeeds) {
  return Math.max(config.catalogScanConcurrency ?? 1, 1);
}

function fullScanStateFromLegacyCatalogState(legacyState: CatalogScanState): FullSyncState {
  return fullSyncStateSchema.parse({
    phase: "scan",
    entityType: legacyState.entityType,
    nextStart: legacyState.nextStart,
    pageSize: legacyState.pageSize,
    pass: legacyState.pass,
    runToken: randomUUID(),
    pagesFetched: legacyState.pagesFetched,
    uniqueIdsSeen: legacyState.uniqueIdsSeen,
    duplicateIdsSeen: legacyState.duplicateIdsSeen,
    hydratedCount: 0,
    updatedAt: legacyState.updatedAt,
  });
}

function defaultFullSyncState(pageSize: number): FullSyncState {
  return fullSyncStateSchema.parse({
    phase: "scan",
    entityType: "artist",
    nextStart: 0,
    pageSize,
    pass: 1,
    runToken: randomUUID(),
    pagesFetched: 0,
    uniqueIdsSeen: 0,
    duplicateIdsSeen: 0,
    hydratedCount: 0,
    updatedAt: new Date().toISOString(),
  });
}

function createHydratePhaseState(current: FullSyncState, config: EffectiveSeeds): FullSyncState {
  return fullSyncStateSchema.parse({
    ...current,
    phase: "hydrate",
    entityType: CATALOG_ENTITY_ORDER[0],
    nextStart: 0,
    pageSize: Math.max(config.concurrency, 1),
    pass: 1,
    updatedAt: new Date().toISOString(),
  });
}

function nextFullScanState(current: FullSyncState, exhausted: boolean, pageItemCount: number, config: EffectiveSeeds) {
  if (!exhausted) {
    return fullSyncStateSchema.parse({
      ...current,
      nextStart: current.nextStart + Math.max(pageItemCount, 1) * current.pageSize,
      updatedAt: new Date().toISOString(),
    });
  }

  const entityIndex = CATALOG_ENTITY_ORDER.indexOf(current.entityType);
  const nextEntityType = CATALOG_ENTITY_ORDER[(entityIndex + 1) % CATALOG_ENTITY_ORDER.length];
  const wrappedToNextPass = nextEntityType === CATALOG_ENTITY_ORDER[0];
  const nextPass = wrappedToNextPass ? current.pass + 1 : current.pass;

  if (nextPass > totalCatalogPasses(config)) {
    return null;
  }

  return fullSyncStateSchema.parse({
    ...current,
    entityType: nextEntityType,
    nextStart: 0,
    pass: nextPass,
    updatedAt: new Date().toISOString(),
  });
}

function nextHydratePhaseState(current: FullSyncState, lastVocadbId: number) {
  return fullSyncStateSchema.parse({
    ...current,
    nextStart: lastVocadbId,
    updatedAt: new Date().toISOString(),
  });
}

function advanceHydrateEntityState(current: FullSyncState) {
  const entityIndex = CATALOG_ENTITY_ORDER.indexOf(current.entityType);
  const nextEntityType = CATALOG_ENTITY_ORDER[entityIndex + 1];
  if (!nextEntityType) {
    return null;
  }

  return fullSyncStateSchema.parse({
    ...current,
    entityType: nextEntityType,
    nextStart: 0,
    updatedAt: new Date().toISOString(),
  });
}

function catalogManifestDisplayName(entityType: EntityType, item: { id: number; name?: string; defaultName?: string }) {
  return item.name || item.defaultName || `${entityType}-${item.id}`;
}

function catalogManifestPublishedAt(item: { publishDate?: string }) {
  return typeof item.publishDate === "string" ? item.publishDate : null;
}

function catalogManifestCreatedAt(item: { createDate?: string }) {
  return typeof item.createDate === "string" ? item.createDate : null;
}

function catalogManifestReleaseDate(item: { releaseDate?: string }) {
  return typeof item.releaseDate === "string" ? item.releaseDate : null;
}

function updateFullCatalogStats(state: FullSyncState | null, config: EffectiveSeeds) {
  if (state === null) {
    stats.catalogPagesFetched = 0;
    stats.catalogUniqueIdsSeen = 0;
    stats.catalogDuplicateIdsSeen = 0;
    stats.catalogPassesCompleted = totalCatalogPasses(config);
    return;
  }

  stats.catalogPagesFetched = state.pagesFetched;
  stats.catalogUniqueIdsSeen = state.uniqueIdsSeen;
  stats.catalogDuplicateIdsSeen = state.duplicateIdsSeen;
  stats.catalogPassesCompleted = state.phase === "scan" ? Math.max(state.pass - 1, 0) : totalCatalogPasses(config);
}

async function initializeFullSyncState(config: EffectiveSeeds, totals: GlobalTotals | null) {
  const existingState = await syncStore.loadFullSyncState();
  if (existingState) {
    return existingState;
  }

  const legacyState = normalizeCatalogScanState(await syncStore.loadCatalogScanState(), config.catalogPageSize, totals);
  if (await syncStore.loadCatalogScanState()) {
    const migratedState = fullScanStateFromLegacyCatalogState(legacyState);
    await syncStore.saveFullSyncState(migratedState);
    await syncStore.clearCatalogScanState();
    return migratedState;
  }

  return defaultFullSyncState(config.catalogPageSize);
}

async function scanCatalogManifestPage(state: FullSyncState, config: EffectiveSeeds) {
  const scanConcurrency = resolveCatalogScanConcurrency(config);
  const pageStarts = Array.from({ length: scanConcurrency }, (_, index) => state.nextStart + index * state.pageSize);
  const pages = await Promise.all(
    pageStarts.map(async (start, index) => {
      const pageStartedAtMs = performance.now();
      const page = await fetchCatalogPage(
        state.entityType,
        {
          start,
          maxResults: state.pageSize,
          sort: "AdditionDate",
          getTotalCount: state.nextStart === 0 && index === 0,
        },
        { timeoutMs: config.idLookupTimeoutMs },
      );
      trackStageDuration("catalogPage", pageStartedAtMs);
      stats.requestCount += 1;
      return { start, page };
    }),
  );

  const orderedPages = pages.sort((left, right) => left.start - right.start);
  const committedPages: typeof orderedPages = [];
  let exhausted = false;

  for (const pageResult of orderedPages) {
    committedPages.push(pageResult);
    if (pageResult.page.ids.length === 0) {
      exhausted = true;
      break;
    }
  }

  const committedItems = committedPages.flatMap((entry) => entry.page.items);
  const lookupStartedAtMs = performance.now();
  const knownVersions = await syncStore.getKnownVersions(
    state.entityType,
    committedItems.map((item) => item.id),
  );
  trackStageDuration("knownVersionLookup", lookupStartedAtMs);

  const uniqueIds = new Set<number>();
  let duplicateIdsInBatch = 0;
  const manifestEntries: CatalogManifestUpsertRecord[] = [];

  for (const item of committedItems) {
    if (uniqueIds.has(item.id)) {
      duplicateIdsInBatch += 1;
      continue;
    }

    uniqueIds.add(item.id);
    const upstreamVersion = typeof item.version === "number" ? item.version : null;
    const knownVersion = knownVersions.get(item.id);
    manifestEntries.push({
      entityType: state.entityType,
      vocadbId: item.id,
      displayName: catalogManifestDisplayName(state.entityType, item),
      upstreamVersion,
      publishedAt: catalogManifestPublishedAt(item),
      createdAt: catalogManifestCreatedAt(item),
      releaseDate: catalogManifestReleaseDate(item),
      lastSeenRunToken: state.runToken,
      needsHydrate: !knownVersions.has(item.id) || (upstreamVersion !== null && knownVersion !== upstreamVersion),
    });
  }

  await syncStore.upsertCatalogManifestEntries(manifestEntries);

  const currentState = fullSyncStateSchema.parse({
    ...state,
    pagesFetched: state.pagesFetched + committedPages.length,
    uniqueIdsSeen: state.uniqueIdsSeen + uniqueIds.size,
    duplicateIdsSeen: state.duplicateIdsSeen + duplicateIdsInBatch,
    updatedAt: new Date().toISOString(),
  });

  return nextFullScanState(currentState, exhausted, committedPages.length, config) ?? createHydratePhaseState(currentState, config);
}

async function hydrateManifestBatch(
  state: FullSyncState,
  config: EffectiveSeeds,
  syncedAt: string,
  failedEntities: FailedEntityRecord[],
  newEntryIds: string[],
  changedEntities: ChangedEntityRecord[],
) {
  const candidates = await syncStore.listHydrateCandidates(state.entityType, state.nextStart, state.pageSize);
  if (candidates.length === 0) {
    return {
      nextState: advanceHydrateEntityState(state),
      shouldStop: false,
    };
  }

  const noopQueue: QueueItem[] = [];
  const noopQueued = new Set<string>();
  const noopSeen = new Set<string>();
  let hydratedCount = 0;

  await Promise.all(
    candidates.map(async (candidate) => {
      const item: QueueItem = {
        entityType: state.entityType,
        vocadbId: candidate.vocadbId,
        depth: 0,
        discoveredFrom: `manifest:${state.entityType}:${state.runToken}`,
        reason: FULL_HYDRATE_REASON,
        knownVersion: candidate.upstreamVersion,
        forcePersist: true,
      };

      try {
        await processQueueItem(item, noopQueue, noopQueued, noopSeen, config, syncedAt, newEntryIds, changedEntities);
        await syncStore.markManifestHydrated(item.entityType, item.vocadbId);
        hydratedCount += 1;
      } catch (error) {
        const message = formatQueueItemError(item, error);
        stats.errors.push(message);
        failedEntities.push({
          ...item,
          error: message,
          failedAt: new Date().toISOString(),
          retryCount: item.retryCount ?? 0,
        });
        await syncStore.markManifestHydrateFailed(item.entityType, item.vocadbId, message);
      }
    }),
  );

  return {
    nextState: fullSyncStateSchema.parse({
      ...nextHydratePhaseState(state, candidates.at(-1)?.vocadbId ?? state.nextStart),
      hydratedCount: state.hydratedCount + hydratedCount,
    }),
    shouldStop: false,
  };
}

async function runFullSync(
  config: EffectiveSeeds,
  totals: GlobalTotals | null,
  syncedAt: string,
  failedEntities: FailedEntityRecord[],
  newEntryIds: string[],
  changedEntities: ChangedEntityRecord[],
  progressReporter: ReturnType<typeof createProgressReporter>,
) {
  let state: FullSyncState | null = await initializeFullSyncState(config, totals);
  updateFullCatalogStats(state, config);
  await syncStore.saveFullSyncState(state);
  progressReporter.setFullSyncState(state);

  while (state !== null) {
    if (state.phase === "scan") {
      progressReporter.tickFullSyncState();
      try {
        state = await scanCatalogManifestPage(state, config);
        updateFullCatalogStats(state, config);
        await syncStore.saveFullSyncState(state);
        progressReporter.setFullSyncState(state);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        stats.errors.push(`full scan paused: ${message}`);
        await syncStore.saveFullSyncState(state);
        progressReporter.setFullSyncState(state);
        return {
          completed: false,
          pendingHydrates: await syncStore.countPendingHydrates(),
          interruptedByError: message,
        };
      }
      continue;
    }

    progressReporter.tickFullSyncState();
    let nextState: FullSyncState | null;
    let shouldStop: boolean;
    try {
      const hydrateResult = await hydrateManifestBatch(state, config, syncedAt, failedEntities, newEntryIds, changedEntities);
      nextState = hydrateResult.nextState;
      shouldStop = hydrateResult.shouldStop;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      stats.errors.push(`full hydrate paused: ${message}`);
      await syncStore.saveFullSyncState(state);
      progressReporter.setFullSyncState(state, await syncStore.countPendingHydrates());
      return {
        completed: false,
        pendingHydrates: await syncStore.countPendingHydrates(),
        interruptedByError: message,
      };
    }

    if (nextState === null) {
      const pendingHydrates = await syncStore.countPendingHydrates();
      if (pendingHydrates === 0) {
        state = null;
        break;
      }

      state = createHydratePhaseState(state, config);
      await syncStore.saveFullSyncState(state);
      return {
        completed: false,
        pendingHydrates,
      };
    }

    state = nextState;
    await syncStore.saveFullSyncState(state);
    progressReporter.setFullSyncState(state, await syncStore.countPendingHydrates());
    if (shouldStop) {
      break;
    }
  }

  await syncStore.clearFullSyncState();
  progressReporter.setFullSyncState(null);
  return {
    completed: true,
    pendingHydrates: 0,
    interruptedByError: null,
  };
}

function enqueueIfAllowed(
  queue: QueueItem[],
  queued: Set<string>,
  seen: Set<string>,
  config: EffectiveSeeds,
  item: QueueItem,
) {
  const key = entityKey(item.entityType, item.vocadbId);
  if (seen.has(key) || queued.has(key)) {
    return false;
  }

  if (!config.allowedEntityTypes.includes(item.entityType)) {
    return false;
  }

  if (item.depth > 0 && hasLimit(config.maxNewEntitiesPerRun) && stats.newEntities >= config.maxNewEntitiesPerRun) {
    stats.limitedEntities += 1;
    return false;
  }

  queue.push(item);
  queued.add(key);
  if (item.depth > 0) {
    stats.newEntities += 1;
  }
  return true;
}

async function expandArtist(item: QueueItem, artist: Artist, queue: QueueItem[], queued: Set<string>, seen: Set<string>, config: EffectiveSeeds) {
  if (!config.expandRelatedDiscographies || item.depth >= config.maxDepth) {
    return;
  }

  const remainingBudget = hasLimit(config.maxNewEntitiesPerRun) ? Math.max(config.maxNewEntitiesPerRun - stats.newEntities, 0) : null;
  if (remainingBudget === 0) {
    return;
  }

  const depthLimit = item.depth === 0 ? config.rootArtistDiscographyLimit : config.relatedArtistDiscographyLimit;
  const songLimit = resolveFetchLimit(depthLimit, remainingBudget);
  const albumLimit = resolveFetchLimit(
    hasLimit(depthLimit) ? Math.max(Math.floor(depthLimit / 2), 1) : null,
    hasLimit(remainingBudget) ? Math.max(Math.floor(remainingBudget / 2), 1) : null,
  );
  const songIds = await fetchSongIdsByArtistId(item.vocadbId, songLimit, {
    timeoutMs: config.idLookupTimeoutMs,
  });
  const albumIds = await fetchAlbumIdsByArtistId(item.vocadbId, albumLimit, {
    timeoutMs: config.idLookupTimeoutMs,
  });

  for (const songId of songIds) {
    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "song",
      vocadbId: songId,
      depth: item.depth + 1,
      discoveredFrom: artist.id,
      reason: "artist-discography",
    });
  }

  for (const albumId of albumIds) {
    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "album",
      vocadbId: albumId,
      depth: item.depth + 1,
      discoveredFrom: artist.id,
      reason: "artist-discography",
    });
  }
}

function expandSong(item: QueueItem, raw: Record<string, unknown>, song: Song, queue: QueueItem[], queued: Set<string>, seen: Set<string>, config: EffectiveSeeds) {
  const artists = Array.isArray(raw.artists) ? raw.artists : [];
  const albums = Array.isArray(raw.albums) ? raw.albums : [];

  for (const artist of artists) {
    const artistId = Number((artist as { artist?: { id?: unknown } }).artist?.id);
    if (!artistId) {
      continue;
    }

    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "artist",
      vocadbId: artistId,
      depth: Math.min(item.depth + 1, config.maxDepth),
      discoveredFrom: song.id,
      reason: "song-artist",
    });
  }

  for (const album of albums) {
    const albumId = Number((album as { id?: unknown }).id);
    if (!albumId) {
      continue;
    }

    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "album",
      vocadbId: albumId,
      depth: Math.min(item.depth + 1, config.maxDepth),
      discoveredFrom: song.id,
      reason: "song-album",
    });
  }
}

function expandAlbum(item: QueueItem, raw: Record<string, unknown>, album: Album, queue: QueueItem[], queued: Set<string>, seen: Set<string>, config: EffectiveSeeds) {
  const artists = Array.isArray(raw.artists) ? raw.artists : [];
  const tracks = Array.isArray(raw.tracks) ? raw.tracks : [];

  for (const artist of artists) {
    const artistId = Number((artist as { artist?: { id?: unknown } }).artist?.id);
    if (!artistId) {
      continue;
    }

    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "artist",
      vocadbId: artistId,
      depth: Math.min(item.depth + 1, config.maxDepth),
      discoveredFrom: album.id,
      reason: "album-artist",
    });
  }

  for (const track of tracks) {
    const songId = Number((track as { song?: { id?: unknown } }).song?.id);
    if (!songId) {
      continue;
    }

    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "song",
      vocadbId: songId,
      depth: Math.min(item.depth + 1, config.maxDepth),
      discoveredFrom: album.id,
      reason: "album-track",
    });
  }
}

async function queueHotEntities(
  queue: QueueItem[],
  queued: Set<string>,
  seen: Set<string>,
  config: EffectiveSeeds,
) {
  if (!hasLimit(config.hotEntityLimit) || config.hotEntityLimit <= 0) {
    return;
  }

  const artistSeedIds = new Set(config.rootArtistIds);
  const hotEntities = await syncStore.getHotEntities(config.hotEntityLimit);
  for (const item of hotEntities) {
    enqueueIfAllowed(queue, queued, seen, config, {
      ...item,
      depth: item.entityType === "artist" && artistSeedIds.has(item.vocadbId) ? 0 : 1,
    });
  }
}

async function queueReconcileShard(
  queue: QueueItem[],
  queued: Set<string>,
  seen: Set<string>,
  config: EffectiveSeeds,
) {
  const artistSeedIds = new Set(config.rootArtistIds);
  const bucketCount = Math.max(config.reconcileBucketCount, 1);
  let state = normalizeReconcileState(await syncStore.loadReconcileState(), bucketCount);
  const attempts = RECONCILE_ENTITY_ORDER.length * bucketCount;

  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const shard = await syncStore.getKnownEntitiesPage(state.entityType, {
      afterVocadbId: state.lastVocadbId,
      limit: config.reconcileShardSize,
      bucket: state.bucket,
      bucketCount: state.bucketCount,
    });

    if (shard.length === 0) {
      state = nextReconcileState(state, true, 0);
      continue;
    }

    for (const entity of shard) {
      enqueueIfAllowed(queue, queued, seen, config, {
        entityType: state.entityType,
        vocadbId: entity.vocadbId,
        depth: state.entityType === "artist" && artistSeedIds.has(entity.vocadbId) ? 0 : 1,
        discoveredFrom: `reconcile:${state.entityType}:${state.bucket}`,
        reason: "reconcile-shard-refresh",
        knownVersion: entity.upstreamVersion ?? null,
      });
    }

    const lastVocadbId = shard.at(-1)?.vocadbId ?? 0;
    const exhausted = shard.length < config.reconcileShardSize;
    return nextReconcileState(state, exhausted, lastVocadbId);
  }

  return state;
}

async function queueFailedEntities(
  queue: QueueItem[],
  queued: Set<string>,
  seen: Set<string>,
  config: EffectiveSeeds,
  legacySnapshot: LegacyStoreSnapshot | null,
) {
  const failed = (await syncStore.hasAnyEntities()) ? await syncStore.loadFailedEntities() : (legacySnapshot?.failedEntities ?? []);
  for (const item of failed) {
    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: item.entityType,
      vocadbId: item.vocadbId,
      depth: item.depth,
      discoveredFrom: item.discoveredFrom,
      reason: "retry-failed",
      knownVersion: item.knownVersion ?? null,
      retryCount: (item.retryCount ?? 0) + 1,
    });
  }
}

async function processQueueItem(
  item: QueueItem,
  queue: QueueItem[],
  queued: Set<string>,
  seen: Set<string>,
  config: EffectiveSeeds,
  syncedAt: string,
  newEntryIds: string[],
  changedEntities: ChangedEntityRecord[],
) {
  const { raw, changed } = await getRawForItem(item);
  const shouldExpand =
    item.reason !== "catalog-enumeration" &&
    item.reason !== FULL_HYDRATE_REASON &&
    (!REFRESH_ONLY_REASONS.has(item.reason) || changed || (mode !== "incremental" && mode !== "incremental-hot"));

  if (!changed && !item.forcePersist) {
    return;
  }

  if (!raw) {
    return;
  }

  const { entity: normalized, isNew, changed: normalizedChanged, previousEntity } = await normalizeAndPersist(item, raw, syncedAt);

  if (isNew) {
    newEntryIds.push(normalized.id);
  }

  if (normalizedChanged) {
    changedEntities.push({
      entityType: normalized.entityType,
      entityId: normalized.id,
      displayName: normalized.displayName,
      previousDisplayName: previousEntity?.displayName ?? null,
      slug: normalized.slug,
      previousSlug: previousEntity?.slug ?? null,
      isNew,
    });
  }

  if (shouldExpand && item.entityType === "artist") {
    await expandArtist(item, normalized as Artist, queue, queued, seen, config);
  } else if (shouldExpand && item.entityType === "song") {
    expandSong(item, raw, normalized as Song, queue, queued, seen, config);
  } else if (shouldExpand) {
    expandAlbum(item, raw, normalized as Album, queue, queued, seen, config);
  }
}

function formatQueueItemError(item: QueueItem, error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return `${item.entityType}:${item.vocadbId}: ${message}`;
}

async function main() {
  const startedAt = new Date().toISOString();
  const syncedAt = new Date().toISOString();
  const seeds = await loadSeeds();
  runtimeConfig = seeds;
  configureVocadbNetworking({
    forceIPv4: parseBooleanEnv("SYNC_FORCE_IPV4") ?? false,
    connections: Math.max(resolveCatalogScanConcurrency(seeds), seeds.concurrency, 8),
  });
  syncStore = new SqliteSyncStore(path.join(dbRoot, "vocadb.sqlite"), dryRun);
  derivedExporter = createFileDerivedExporter(derivedRoot, dryRun);
  metaExporter = createFileDerivedExporter(path.join(rawRoot, "meta"), dryRun);
  await syncStore.saveSeedsSnapshot(seeds);
  const progressEnabled = parseBooleanEnv("SYNC_LOG_PROGRESS") ?? true;
  const progressIntervalMs = parseIntegerEnv("SYNC_LOG_PROGRESS_INTERVAL_MS") ?? 10000;
  const progressReporter = createProgressReporter(progressEnabled, progressIntervalMs);

  const queue: QueueItem[] = [];
  const queued = new Set<string>();
  const seen = new Set<string>();
  const failedEntities: FailedEntityRecord[] = [];
  const newEntryIds: string[] = [];
  const changedEntities: ChangedEntityRecord[] = [];
  let legacySnapshot: LegacyStoreSnapshot | null = null;
  let nextReconcileCursor: ReconcileState | null = null;
  let globalTotals: GlobalTotals | null = null;
  let fullSyncCompleted = mode !== "full";

  if (mode !== "full" && mode !== "derive" && !(await syncStore.hasAnyEntities())) {
    legacySnapshot = await loadLegacySnapshot(seeds);
    await syncStore.importLegacySnapshot(legacySnapshot);
  }

  if (mode !== "derive") {
    const totalTimeoutMs = Math.max(seeds.probeTimeoutMs, seeds.idLookupTimeoutMs, 30000);
    const [artistResult, songResult, albumResult] = await Promise.allSettled([
      fetchEntityTotalCount("artist", { timeoutMs: totalTimeoutMs }),
      fetchEntityTotalCount("song", { timeoutMs: totalTimeoutMs }),
      fetchEntityTotalCount("album", { timeoutMs: totalTimeoutMs }),
    ]);
    const resolvedTotals: PartialGlobalTotals = {};

    if (artistResult.status === "fulfilled") {
      resolvedTotals.artist = artistResult.value;
    } else {
      stats.errors.push(`artist totalCount preflight failed: ${artistResult.reason instanceof Error ? artistResult.reason.message : String(artistResult.reason)}`);
    }
    if (songResult.status === "fulfilled") {
      resolvedTotals.song = songResult.value;
    } else {
      stats.errors.push(`song totalCount preflight failed: ${songResult.reason instanceof Error ? songResult.reason.message : String(songResult.reason)}`);
    }
    if (albumResult.status === "fulfilled") {
      resolvedTotals.album = albumResult.value;
    } else {
      stats.errors.push(`album totalCount preflight failed: ${albumResult.reason instanceof Error ? albumResult.reason.message : String(albumResult.reason)}`);
    }

    if (
      typeof resolvedTotals.artist === "number" &&
      typeof resolvedTotals.song === "number" &&
      typeof resolvedTotals.album === "number"
    ) {
      globalTotals = {
        artist: resolvedTotals.artist,
        song: resolvedTotals.song,
        album: resolvedTotals.album,
      };
    }

    progressReporter.setGlobalTotals(Object.keys(resolvedTotals).length > 0 ? resolvedTotals : null);
  }

  if (mode !== "full" && mode !== "derive") {
    for (const rootArtistId of seeds.rootArtistIds) {
      enqueueIfAllowed(queue, queued, seen, seeds, {
        entityType: "artist",
        vocadbId: rootArtistId,
        depth: 0,
        discoveredFrom: "seed",
        reason: "root-artist",
      });
    }
  }

  if (mode === "incremental" || mode === "incremental-hot") {
    if (seeds.retryFailedOnIncremental) {
      await queueFailedEntities(queue, queued, seen, seeds, legacySnapshot);
    }
    await queueHotEntities(queue, queued, seen, seeds);
  }

  if (mode === "incremental" || mode === "reconcile-shard") {
    nextReconcileCursor = await queueReconcileShard(queue, queued, seen, seeds);
  }

  progressReporter.start(seen, queued);

  try {
    if (mode === "derive") {
      const recoveredChanges = await loadChangedEntitiesSinceLastRun();
      changedEntities.push(...recoveredChanges);
      for (const changed of recoveredChanges) {
        if (changed.isNew) {
          newEntryIds.push(changed.entityId);
        }
      }
    } else if (mode === "full") {
      const fullSyncResult = await runFullSync(seeds, globalTotals, syncedAt, failedEntities, newEntryIds, changedEntities, progressReporter);
      fullSyncCompleted = fullSyncResult.completed;
      if (!fullSyncResult.completed) {
        fatalErrors.push(
          fullSyncResult.interruptedByError
            ? `Full sync paused after retriable failure: ${fullSyncResult.interruptedByError}`
            : `Full hydrate not finished, ${fullSyncResult.pendingHydrates} manifest rows still need hydrate.`,
        );
      }
    } else {
      while (queue.length > 0) {
        const batch: QueueItem[] = [];

        while (queue.length > 0 && batch.length < seeds.concurrency) {
          const item = queue.shift()!;
          const key = entityKey(item.entityType, item.vocadbId);
          if (seen.has(key)) {
            continue;
          }

          seen.add(key);
          queued.delete(key);
          batch.push(item);
        }

        progressReporter.setActive(batch.length, seen, queued);

        await Promise.all(
          batch.map(async (item) => {
            try {
              await processQueueItem(item, queue, queued, seen, seeds, syncedAt, newEntryIds, changedEntities);
            } catch (error) {
              const message = formatQueueItemError(item, error);
              stats.errors.push(message);
              failedEntities.push({
                ...item,
                error: message,
                failedAt: new Date().toISOString(),
                retryCount: item.retryCount ?? 0,
              });
              if (item.depth === 0 && item.reason !== "catalog-enumeration") {
                fatalErrors.push(message);
              }
            }
          }),
        );

        progressReporter.batchCompleted(batch.length, seen, queued);
      }
    }

    const shouldBuildDerived =
      fullSyncCompleted && (mode === "derive" ? changedEntities.length > 0 : stats.normalizedUpdated > 0);
    const derivedSnapshot = shouldBuildDerived ? await buildDerived(changedEntities) : null;
    const sortedNewEntries =
      derivedSnapshot === null
        ? []
        : (
            await Promise.all([...new Set(newEntryIds)].map((entityId) => syncStore.getEntitySummaryById(entityId)))
          )
            .filter((entry): entry is EntitySummary => entry !== null)
            .sort((left, right) => right.syncedAt.localeCompare(left.syncedAt) || left.slug.localeCompare(right.slug));

    if (mode !== "full" || fullSyncCompleted) {
      await syncStore.saveNewEntries(sortedNewEntries);
      await writeArtifactIfChanged(derivedExporter, "meta/new-entries.json", sortedNewEntries, "derivedUpdated");
    }
    await syncStore.saveFailedEntities(failedEntities);
    if (nextReconcileCursor) {
      await syncStore.saveReconcileState(nextReconcileCursor);
    }

    if (!seeds.storeRaw) {
      await clearRawEntityCaches();
    }

    const finishedAt = new Date().toISOString();
    const lastRun = lastRunSchema.parse({
      ...stats,
      startedAt,
      finishedAt,
    });

    await syncStore.saveLastRun(lastRun);
    await writeArtifactIfChanged(metaExporter, "last-run.json", lastRun, "rawUpdated");
  } finally {
    progressReporter.stop(seen, queued);
    syncStore.close();
    await closeVocadbNetworking();
  }

  console.log(
    stableStringify({
      mode,
      dryRun,
      requestCount: stats.requestCount,
      probeRequests: stats.probeRequests,
      probeHits: stats.probeHits,
      fullFetches: stats.fullFetches,
      rawUpdated: stats.rawUpdated,
      normalizedUpdated: stats.normalizedUpdated,
      derivedUpdated: stats.derivedUpdated,
      newEntities: stats.newEntities,
      limitedEntities: stats.limitedEntities,
      catalogPagesFetched: stats.catalogPagesFetched,
      catalogUniqueIdsSeen: stats.catalogUniqueIdsSeen,
      catalogDuplicateIdsSeen: stats.catalogDuplicateIdsSeen,
      catalogPassesCompleted: stats.catalogPassesCompleted,
      errorCount: stats.errors.length,
      fatalErrorCount: fatalErrors.length,
      sampleErrors: stats.errors.slice(0, 5),
    }),
  );

  if (fatalErrors.length > 0) {
    process.exitCode = 1;
  }
}

await main();
