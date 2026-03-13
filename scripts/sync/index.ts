import { brotliCompressSync, brotliDecompressSync, constants as zlibConstants } from "node:zlib";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { hashJson, stableStringify } from "../../src/lib/json";
import {
  albumSchema,
  artistSchema,
  graphSummarySchema,
  lastRunSchema,
  seedsSchema,
  songSchema,
  type Album,
  type Artist,
  type EntityType,
  type ExternalLink,
  type LastRun,
  type Seeds,
  type Song,
} from "../../src/lib/models";
import { slugify } from "../../src/lib/slug";
import {
  fetchAlbumById,
  fetchAlbumMetaById,
  fetchAlbumIdsByArtistId,
  fetchArtistById,
  fetchArtistMetaById,
  fetchSongById,
  fetchSongMetaById,
  fetchSongIdsByArtistId,
  type QueueItem,
  type SyncMode,
} from "../../src/lib/vocadb";

const repoRoot = process.cwd();
const dataRoot = path.join(repoRoot, "data");
const rawRoot = path.join(dataRoot, "raw", "vocadb");
const normalizedRoot = path.join(dataRoot, "normalized");
const derivedRoot = path.join(dataRoot, "derived");

const args = process.argv.slice(2);
const mode = (args[0] ?? "incremental") as SyncMode;
const dryRun = args.includes("--dry-run");

type Stats = Omit<LastRun, "startedAt" | "finishedAt">;
type RawEnvelope = {
  format: "br";
  fetchedAt: string;
  entityType: EntityType;
  vocadbId: number;
  version: number | null;
  payload: Record<string, unknown>;
};
type EffectiveSeeds = Omit<Seeds, "modes">;
type FailedEntityRecord = QueueItem & {
  error: string;
  failedAt: string;
};
type EntitySummary = {
  id: string;
  entityType: EntityType;
  slug: string;
  displayName: string;
  url: string;
  sourceUrl: string;
  syncedAt: string;
  upstreamVersion?: number | null;
  year?: number | null;
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
  errors: [],
};
const fatalErrors: string[] = [];
let runtimeConfig: EffectiveSeeds;

function entityKey(entityType: EntityType, vocadbId: number) {
  return `${entityType}:${vocadbId}`;
}

function rawFilePath(entityType: EntityType, vocadbId: number) {
  return path.join(rawRoot, `${entityType}s`, `${vocadbId}.json.br`);
}

function legacyRawFilePath(entityType: EntityType, vocadbId: number) {
  return path.join(rawRoot, `${entityType}s`, `${vocadbId}.json`);
}

function normalizedFilePath(entityType: EntityType, slug: string) {
  return path.join(normalizedRoot, `${entityType}s`, `${slug}.json`);
}

function normalizedId(entityType: EntityType, vocadbId: number) {
  return `${entityType}-${vocadbId}`;
}

function sourceUrl(entityType: EntityType, vocadbId: number) {
  const prefix = entityType === "artist" ? "Ar" : entityType === "song" ? "S" : "Al";
  return `https://vocadb.net/${prefix}/${vocadbId}`;
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

      return {
        url,
        label: description || new URL(url).hostname,
        kind: category,
        service: new URL(url).hostname.replace(/^www\./, ""),
        official: Boolean(link.disabled === false || link.disabled === undefined),
      } satisfies ExternalLink;
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

      return {
        url,
        label: typeof pv.name === "string" && pv.name ? pv.name : String(pv.service ?? "PV"),
        kind: typeof pv.pvType === "string" ? pv.pvType.toLowerCase() : "pv",
        service: typeof pv.service === "string" ? pv.service.toLowerCase() : "external",
        official: pv.pvType === "Original",
      } satisfies ExternalLink;
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

async function ensureDir(targetPath: string) {
  await mkdir(path.dirname(targetPath), { recursive: true });
}

async function readJsonFile<T>(targetPath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(targetPath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

async function readTextFile(targetPath: string) {
  try {
    return await readFile(targetPath, "utf8");
  } catch {
    return null;
  }
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

async function writeJsonIfChanged(targetPath: string, value: unknown, counter: "rawUpdated" | "normalizedUpdated" | "derivedUpdated") {
  const nextContent = stableStringify(value);
  const previousSerialized = await readTextFile(targetPath);

  if (previousSerialized === nextContent) {
    return false;
  }

  if (!dryRun) {
    await ensureDir(targetPath);
    await writeFile(targetPath, nextContent, "utf8");
  }

  stats[counter] += 1;
  return true;
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
  const nextCompressed = brotliCompressSync(Buffer.from(nextContent, "utf8"), {
    params: {
      [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
    },
  });
  const previousEnvelope = await readCompressedJsonFile<RawEnvelope>(targetPath);

  if (previousEnvelope) {
    const previousContent = stableStringify(previousEnvelope);
    if (previousContent === nextContent) {
      return false;
    }
  }

  if (!dryRun) {
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
  const seedsPath = path.join(rawRoot, "meta", "seeds.json");
  const seeds = await readJsonFile<Partial<Seeds>>(seedsPath, {});
  return resolveSeedsForMode(seedsSchema.parse(seeds), mode);
}

async function loadFailedEntities() {
  return readJsonFile<FailedEntityRecord[]>(path.join(rawRoot, "meta", "failed-entities.json"), []);
}

async function listNormalizedEntities<T>(entityType: EntityType): Promise<T[]> {
  const targetDir = path.join(normalizedRoot, `${entityType}s`);
  try {
    const files = await readdir(targetDir);
    const values = await Promise.all(
      files
        .filter((file) => file.endsWith(".json"))
        .map((file) => readJsonFile<T>(path.join(targetDir, file), null as T)),
    );
    return values.filter(Boolean);
  } catch {
    return [];
  }
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
    url: `/artists/${slug}/`,
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
    url: `/songs/${slug}/`,
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
    url: `/albums/${slug}/`,
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
  stats.fullFetches += 1;
  stats.requestCount += 1;
  if (item.entityType === "artist") {
    return fetchArtistById(item.vocadbId, { timeoutMs: runtimeConfig.fetchTimeoutMs });
  }
  if (item.entityType === "song") {
    return fetchSongById(item.vocadbId, { timeoutMs: runtimeConfig.fetchTimeoutMs });
  }
  return fetchAlbumById(item.vocadbId, { timeoutMs: runtimeConfig.fetchTimeoutMs });
}

async function fetchProbeMeta(item: QueueItem) {
  stats.probeRequests += 1;
  stats.requestCount += 1;

  if (item.entityType === "artist") {
    return fetchArtistMetaById(item.vocadbId, { timeoutMs: runtimeConfig.probeTimeoutMs });
  }
  if (item.entityType === "song") {
    return fetchSongMetaById(item.vocadbId, { timeoutMs: runtimeConfig.probeTimeoutMs });
  }
  return fetchAlbumMetaById(item.vocadbId, { timeoutMs: runtimeConfig.probeTimeoutMs });
}

async function getRawForItem(item: QueueItem) {
  const stored = runtimeConfig.storeRaw ? await readStoredRaw(item.entityType, item.vocadbId) : null;
  const knownVersion = stored?.version ?? item.knownVersion ?? null;

  if (knownVersion !== null) {
    const probe = await fetchProbeMeta(item);
    if (typeof probe.version === "number" && probe.version === knownVersion) {
      stats.probeHits += 1;
      if (stored && !stored.isCompressed && runtimeConfig.storeRaw) {
        await writeCompressedRawIfChanged(item.entityType, item.vocadbId, stored.payload, new Date().toISOString());
      }
      return {
        raw: stored?.payload ?? null,
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
  if (item.entityType === "artist") {
    const artist = buildArtist(raw, syncedAt);
    const targetPath = normalizedFilePath("artist", artist.slug);
    const existedBefore = (await readTextFile(targetPath)) !== null;
    await writeJsonIfChanged(targetPath, artist, "normalizedUpdated");
    return {
      entity: artist,
      isNew: !existedBefore,
    };
  }
  if (item.entityType === "song") {
    const song = buildSong(raw, syncedAt);
    const targetPath = normalizedFilePath("song", song.slug);
    const existedBefore = (await readTextFile(targetPath)) !== null;
    await writeJsonIfChanged(targetPath, song, "normalizedUpdated");
    return {
      entity: song,
      isNew: !existedBefore,
    };
  }

  const album = buildAlbum(raw, syncedAt);
  const targetPath = normalizedFilePath("album", album.slug);
  const existedBefore = (await readTextFile(targetPath)) !== null;
  await writeJsonIfChanged(targetPath, album, "normalizedUpdated");
  return {
    entity: album,
    isNew: !existedBefore,
  };
}

function summarizeEntity(entity: Artist | Song | Album): EntitySummary {
  const year = "year" in entity && typeof entity.year === "number" ? entity.year : null;
  return {
    id: entity.id,
    entityType: entity.entityType,
    slug: entity.slug,
    displayName: entity.displayName,
    url: entity.url,
    sourceUrl: entity.sourceUrl,
    syncedAt: entity.syncedAt,
    upstreamVersion: entity.upstreamVersion ?? null,
    year,
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
    return;
  }

  if (!config.allowedEntityTypes.includes(item.entityType)) {
    return;
  }

  if (stats.newEntities >= config.maxNewEntitiesPerRun && item.depth > 0) {
    stats.limitedEntities += 1;
    return;
  }

  queue.push(item);
  queued.add(key);
  if (item.depth > 0) {
    stats.newEntities += 1;
  }
}

async function expandArtist(item: QueueItem, artist: Artist, queue: QueueItem[], queued: Set<string>, seen: Set<string>, config: EffectiveSeeds) {
  if (!config.expandRelatedDiscographies || item.depth >= config.maxDepth) {
    return;
  }

  const remainingBudget = Math.max(config.maxNewEntitiesPerRun - stats.newEntities, 0);
  const depthLimit = item.depth === 0 ? config.rootArtistDiscographyLimit : config.relatedArtistDiscographyLimit;
  const localLimit = Math.max(Math.min(remainingBudget || depthLimit, depthLimit), 2);
  const songIds = await fetchSongIdsByArtistId(item.vocadbId, localLimit, {
    timeoutMs: config.idLookupTimeoutMs,
  });
  const albumIds = await fetchAlbumIdsByArtistId(item.vocadbId, Math.max(Math.floor(localLimit / 2), 2), {
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

async function buildDerived() {
  const [artists, songs, albums] = await Promise.all([
    listNormalizedEntities<Artist>("artist"),
    listNormalizedEntities<Song>("song"),
    listNormalizedEntities<Album>("album"),
  ]);

  const sortedArtists = artists.sort((a, b) => a.slug.localeCompare(b.slug));
  const sortedSongs = songs.sort((a, b) => a.slug.localeCompare(b.slug));
  const sortedAlbums = albums.sort((a, b) => a.slug.localeCompare(b.slug));

  const songsByYear = Object.entries(
    sortedSongs.reduce<Record<string, Song[]>>((acc, song) => {
      const year = song.year ? String(song.year) : "unknown";
      acc[year] ??= [];
      acc[year].push(song);
      return acc;
    }, {}),
  )
    .sort(([a], [b]) => a.localeCompare(b))
    .reduce<Record<string, Song[]>>((acc, [year, values]) => {
      acc[year] = values.sort((left, right) => left.displayName.localeCompare(right.displayName));
      return acc;
    }, {});

  const albumsByYear = Object.entries(
    sortedAlbums.reduce<Record<string, Album[]>>((acc, album) => {
      const year = album.year ? String(album.year) : "unknown";
      acc[year] ??= [];
      acc[year].push(album);
      return acc;
    }, {}),
  )
    .sort(([a], [b]) => a.localeCompare(b))
    .reduce<Record<string, Album[]>>((acc, [year, values]) => {
      acc[year] = values.sort((left, right) => left.displayName.localeCompare(right.displayName));
      return acc;
    }, {});

  const artistToSongs = sortedArtists.reduce<Record<string, string[]>>((acc, artist) => {
    acc[artist.id] = [...artist.songIds].sort();
    return acc;
  }, {});

  const artistToAlbums = sortedArtists.reduce<Record<string, string[]>>((acc, artist) => {
    acc[artist.id] = [...artist.albumIds].sort();
    return acc;
  }, {});

  const recentlyUpdated = [...sortedArtists, ...sortedSongs, ...sortedAlbums]
    .sort((a, b) => b.syncedAt.localeCompare(a.syncedAt))
    .slice(0, 25);

  const summary = graphSummarySchema.parse({
    syncedAt: new Date().toISOString(),
    artistCount: sortedArtists.length,
    songCount: sortedSongs.length,
    albumCount: sortedAlbums.length,
  });

  await Promise.all([
    writeJsonIfChanged(path.join(derivedRoot, "artists-by-slug.json"), sortedArtists, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "songs-by-slug.json"), sortedSongs, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "albums-by-slug.json"), sortedAlbums, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "songs-by-year.json"), songsByYear, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "albums-by-year.json"), albumsByYear, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "artist-to-songs.json"), artistToSongs, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "artist-to-albums.json"), artistToAlbums, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "recently-updated.json"), recentlyUpdated, "derivedUpdated"),
    writeJsonIfChanged(path.join(derivedRoot, "graph-summary.json"), summary, "derivedUpdated"),
  ]);
}

async function queueKnownEntities(queue: QueueItem[], queued: Set<string>, seen: Set<string>, config: EffectiveSeeds) {
  const [artists, songs, albums] = await Promise.all([
    listNormalizedEntities<Artist>("artist"),
    listNormalizedEntities<Song>("song"),
    listNormalizedEntities<Album>("album"),
  ]);

  for (const artist of artists) {
    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "artist",
      vocadbId: artist.vocadbId,
      depth: artist.vocadbId === 1 ? 0 : 1,
      discoveredFrom: "normalized-cache",
      reason: "incremental-refresh",
      knownVersion: artist.upstreamVersion ?? null,
    });
  }

  for (const song of songs) {
    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "song",
      vocadbId: song.vocadbId,
      depth: 1,
      discoveredFrom: "normalized-cache",
      reason: "incremental-refresh",
      knownVersion: song.upstreamVersion ?? null,
    });
  }

  for (const album of albums) {
    enqueueIfAllowed(queue, queued, seen, config, {
      entityType: "album",
      vocadbId: album.vocadbId,
      depth: 1,
      discoveredFrom: "normalized-cache",
      reason: "incremental-refresh",
      knownVersion: album.upstreamVersion ?? null,
    });
  }
}

async function queueFailedEntities(queue: QueueItem[], queued: Set<string>, seen: Set<string>, config: EffectiveSeeds) {
  const failed = await loadFailedEntities();
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
  newEntries: EntitySummary[],
) {
  const { raw, changed } = await getRawForItem(item);
  const shouldExpand = mode !== "incremental" || changed || item.reason !== "incremental-refresh";

  if (!changed) {
    return;
  }

  if (!raw) {
    return;
  }

  const { entity: normalized, isNew } = await normalizeAndPersist(item, raw, syncedAt);

  if (isNew) {
    newEntries.push(summarizeEntity(normalized));
  }

  if (shouldExpand && item.entityType === "artist") {
    await expandArtist(item, normalized as Artist, queue, queued, seen, config);
  } else if (shouldExpand && item.entityType === "song") {
    expandSong(item, raw, normalized as Song, queue, queued, seen, config);
  } else if (shouldExpand) {
    expandAlbum(item, raw, normalized as Album, queue, queued, seen, config);
  }
}

async function main() {
  const startedAt = new Date().toISOString();
  const syncedAt = new Date().toISOString();
  const seeds = await loadSeeds();
  runtimeConfig = seeds;

  const queue: QueueItem[] = [];
  const queued = new Set<string>();
  const seen = new Set<string>();
  const failedEntities: FailedEntityRecord[] = [];
  const newEntries: EntitySummary[] = [];

  for (const rootArtistId of seeds.rootArtistIds) {
    enqueueIfAllowed(queue, queued, seen, seeds, {
      entityType: "artist",
      vocadbId: rootArtistId,
      depth: 0,
      discoveredFrom: "seed",
      reason: "root-artist",
    });
  }

  if (mode === "incremental") {
    if (seeds.retryFailedOnIncremental) {
      await queueFailedEntities(queue, queued, seen, seeds);
    }
    await queueKnownEntities(queue, queued, seen, seeds);
  }

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

    await Promise.all(
      batch.map(async (item) => {
        try {
          await processQueueItem(item, queue, queued, seen, seeds, syncedAt, newEntries);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          stats.errors.push(message);
          failedEntities.push({
            ...item,
            error: message,
            failedAt: new Date().toISOString(),
            retryCount: item.retryCount ?? 0,
          });
          if (item.depth === 0) {
            fatalErrors.push(message);
          }
        }
      }),
    );
  }

  if (stats.normalizedUpdated > 0) {
    await buildDerived();
  }

  await writeJsonIfChanged(
    path.join(derivedRoot, "new-entries.json"),
    newEntries.sort((left, right) => right.syncedAt.localeCompare(left.syncedAt) || left.slug.localeCompare(right.slug)),
    "derivedUpdated",
  );

  await writeJsonIfChanged(path.join(rawRoot, "meta", "failed-entities.json"), failedEntities, "rawUpdated");

  if (!seeds.storeRaw) {
    await clearRawEntityCaches();
  }

  const finishedAt = new Date().toISOString();
  const lastRun = lastRunSchema.parse({
    ...stats,
    startedAt,
    finishedAt,
  });

  await writeJsonIfChanged(path.join(rawRoot, "meta", "last-run.json"), lastRun, "rawUpdated");

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
