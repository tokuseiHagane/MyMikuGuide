import { readFile } from "node:fs/promises";
import path from "node:path";
import type {
  AlbumDetail,
  AlbumRouteManifest,
  AlbumSummaryPage,
  ArtistDetail,
  ArtistRouteManifest,
  ArtistSummaryPage,
  EntityRouteManifest,
  EntitySummary,
  EntityType,
  GraphSummary,
  LastRun,
  SongDetail,
  SongRouteManifest,
  SongSummaryPage,
} from "./models";

const repoRoot = process.cwd();
const dataDir = path.join(repoRoot, "data");
const defaultPageSize = 48;

async function readJson<T>(relativePath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(path.join(dataDir, relativePath), "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
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
  return `derived/summary/${pluralEntityType(entityType)}/page-${String(pageNumber).padStart(4, "0")}.json`;
}

function routeManifestPath(entityType: EntityType) {
  return `derived/meta/routes/${pluralEntityType(entityType)}.json`;
}

function detailPath(entityType: EntityType, slug: string) {
  return `derived/detail/${entityType}/${slug}.json`;
}

function emptyRouteManifest(entityType: EntityType): EntityRouteManifest {
  return {
    entityType,
    pageSize: defaultPageSize,
    totalItems: 0,
    totalPages: 1,
    items: [],
  } as EntityRouteManifest;
}

function emptySummaryPage(entityType: EntityType, pageNumber: number) {
  return {
    entityType,
    pageNumber,
    pageSize: defaultPageSize,
    totalItems: 0,
    totalPages: 1,
    items: [],
  } as ArtistSummaryPage | SongSummaryPage | AlbumSummaryPage;
}

export async function loadRouteManifest(entityType: "artist"): Promise<ArtistRouteManifest>;
export async function loadRouteManifest(entityType: "song"): Promise<SongRouteManifest>;
export async function loadRouteManifest(entityType: "album"): Promise<AlbumRouteManifest>;
export async function loadRouteManifest(entityType: EntityType): Promise<EntityRouteManifest> {
  return readJson<EntityRouteManifest>(routeManifestPath(entityType), emptyRouteManifest(entityType));
}

export async function loadSummaryPage(entityType: "artist", pageNumber: number): Promise<ArtistSummaryPage>;
export async function loadSummaryPage(entityType: "song", pageNumber: number): Promise<SongSummaryPage>;
export async function loadSummaryPage(entityType: "album", pageNumber: number): Promise<AlbumSummaryPage>;
export async function loadSummaryPage(entityType: EntityType, pageNumber: number) {
  return readJson<ArtistSummaryPage | SongSummaryPage | AlbumSummaryPage>(
    summaryPagePath(entityType, pageNumber),
    emptySummaryPage(entityType, pageNumber),
  );
}

export async function loadSummaryPageItems(entityType: EntityType, pageNumber: number) {
  if (entityType === "artist") {
    return (await loadSummaryPage("artist", pageNumber)).items;
  }
  if (entityType === "song") {
    return (await loadSummaryPage("song", pageNumber)).items;
  }
  return (await loadSummaryPage("album", pageNumber)).items;
}

export async function loadEntityDetail(entityType: "artist", slug: string): Promise<ArtistDetail | null>;
export async function loadEntityDetail(entityType: "song", slug: string): Promise<SongDetail | null>;
export async function loadEntityDetail(entityType: "album", slug: string): Promise<AlbumDetail | null>;
export async function loadEntityDetail(entityType: EntityType, slug: string) {
  return readJson<ArtistDetail | SongDetail | AlbumDetail | null>(detailPath(entityType, slug), null);
}

export async function loadGraphSummary() {
  return readJson<GraphSummary>("derived/meta/graph-summary.json", {
    syncedAt: "",
    artistCount: 0,
    songCount: 0,
    albumCount: 0,
    summaryPageSize: defaultPageSize,
  });
}

export async function loadRecentlyUpdated() {
  return readJson<EntitySummary[]>("derived/meta/recently-updated.json", []);
}

export async function loadUpdatedToday() {
  return readJson<EntitySummary[]>("derived/meta/updated-today.json", []);
}

export async function loadNewEntries() {
  return readJson<EntitySummary[]>("derived/meta/new-entries.json", []);
}

export type StatsData = {
  totals: { artists: number; songs: number; albums: number };
  yearDistribution: { year: number; songs: number; albums: number }[];
  topTags: { tag: string; count: number }[];
  artistTypes: { type: string; count: number }[];
  songTypes: { type: string; count: number }[];
  albumTypes: { type: string; count: number }[];
  generatedAt: string;
};

export type TagsIndex = {
  totalTags: number;
  totalSongs: number;
  tags: { tag: string; count: number; slugs: string[] }[];
  generatedAt: string;
};

export type YearsIndex = {
  years: { year: number; songs: number; albums: number; total: number }[];
  totalYears: number;
  generatedAt: string;
};

export async function loadStats() {
  return readJson<StatsData>("derived/meta/stats.json", {
    totals: { artists: 0, songs: 0, albums: 0 },
    yearDistribution: [],
    topTags: [],
    artistTypes: [],
    songTypes: [],
    albumTypes: [],
    generatedAt: "",
  });
}

export async function loadTagsIndex() {
  return readJson<TagsIndex>("derived/meta/tags-index.json", {
    totalTags: 0,
    totalSongs: 0,
    tags: [],
    generatedAt: "",
  });
}

export async function loadYearsIndex() {
  return readJson<YearsIndex>("derived/meta/years-index.json", {
    years: [],
    totalYears: 0,
    generatedAt: "",
  });
}

export async function loadLastRun() {
  return readJson<LastRun>("raw/vocadb/meta/last-run.json", {
    mode: "incremental",
    dryRun: false,
    startedAt: "",
    finishedAt: "",
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
  });
}
