import { readFile } from "node:fs/promises";
import path from "node:path";
import type { Album, Artist, GraphSummary, LastRun, Song } from "./models";

export type SiteEntitySummary = {
  id: string;
  entityType: "artist" | "song" | "album";
  slug: string;
  displayName: string;
  url: string;
  sourceUrl: string;
  syncedAt: string;
  upstreamVersion?: number | null;
  year?: number | null;
};

const repoRoot = process.cwd();
const dataDir = path.join(repoRoot, "data");

async function readJson<T>(relativePath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(path.join(dataDir, relativePath), "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export async function loadArtists() {
  return readJson<Artist[]>("derived/artists-by-slug.json", []);
}

export async function loadSongs() {
  return readJson<Song[]>("derived/songs-by-slug.json", []);
}

export async function loadAlbums() {
  return readJson<Album[]>("derived/albums-by-slug.json", []);
}

export async function loadGraphSummary() {
  return readJson<GraphSummary>("derived/graph-summary.json", {
    syncedAt: "",
    artistCount: 0,
    songCount: 0,
    albumCount: 0,
  });
}

export async function loadRecentlyUpdated() {
  return readJson<SiteEntitySummary[]>("derived/recently-updated.json", []);
}

export async function loadNewEntries() {
  return readJson<SiteEntitySummary[]>("derived/new-entries.json", []);
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
    errors: [],
  });
}
