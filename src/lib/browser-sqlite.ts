import { createDbWorker, type WorkerHttpvfs } from "sql.js-httpvfs";
import type { SplitFileConfig } from "sql.js-httpvfs/dist/sqlite.worker";
import type {
  Album,
  AlbumDetail,
  AlbumSummary,
  AlbumTrackDetail,
  Artist,
  ArtistDetail,
  ArtistSummary,
  EntityType,
  Song,
  SongDetail,
  SongSummary,
} from "./models";

const workerUrl = new URL("sql.js-httpvfs/dist/sqlite.worker.js", import.meta.url);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);
const BASE_URL = import.meta.env.BASE_URL;
const MAX_BYTES_TO_READ = 512 * 1024 * 1024;

export type DbSnapshotManifest = {
  available: boolean;
  provider: "pages-sqlite";
  version: string | null;
  configUrl: string | null;
  databaseUrl: string | null;
  sha256: string | null;
  bytes: number | null;
  pageSize: number | null;
  updatedAt: string | null;
};

type PayloadRow = {
  payload_json: string;
};

type ArtistSummaryRow = PayloadRow & {
  song_count: number | null;
  album_count: number | null;
};

type SongSummaryRow = PayloadRow & {
  artist_count: number | null;
  album_count: number | null;
};

type AlbumSummaryRow = PayloadRow & {
  track_count: number | null;
};

type AlbumTrackRow = {
  song_id: string;
  track_number: number;
  disc_number: number;
  track_name: string;
  payload_json: string | null;
  artist_count: number | null;
  album_count: number | null;
};

let manifestPromise: Promise<DbSnapshotManifest> | null = null;
let workerPromise: Promise<WorkerHttpvfs | null> | null = null;
const manifestCandidates = ["/meta/db-snapshot.json", "/meta/db-snapshot.local.json"];

function withBase(path: string) {
  if (!path.startsWith("/")) {
    return path;
  }

  const normalizedBase = BASE_URL.endsWith("/") ? BASE_URL.slice(0, -1) : BASE_URL;
  return `${normalizedBase}${path}`;
}

function resolveSiteUrl(pathOrUrl: string) {
  if (/^[a-z]+:\/\//i.test(pathOrUrl)) {
    return pathOrUrl;
  }

  if (pathOrUrl.startsWith("/")) {
    return new URL(withBase(pathOrUrl), window.location.origin).toString();
  }

  return new URL(pathOrUrl, window.location.href).toString();
}

function numberOrZero(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function parsePayload<T>(payloadJson: string) {
  return JSON.parse(payloadJson) as T;
}

function mapArtistSummaryRow(row: ArtistSummaryRow): ArtistSummary {
  const artist = parsePayload<Artist>(row.payload_json);
  return {
    id: artist.id,
    entityType: "artist",
    slug: artist.slug,
    displayName: artist.displayName,
    additionalNames: artist.additionalNames,
    url: artist.url,
    sourceUrl: artist.sourceUrl,
    syncedAt: artist.syncedAt,
    upstreamVersion: artist.upstreamVersion ?? null,
    primaryImage: artist.primaryImage ?? null,
    artistType: artist.artistType,
    descriptionShort: artist.descriptionShort,
    songCount: numberOrZero(row.song_count),
    albumCount: numberOrZero(row.album_count),
  };
}

function mapSongSummaryRow(row: SongSummaryRow): SongSummary {
  const song = parsePayload<Song>(row.payload_json);
  return {
    id: song.id,
    entityType: "song",
    slug: song.slug,
    displayName: song.displayName,
    additionalNames: song.additionalNames,
    url: song.url,
    sourceUrl: song.sourceUrl,
    syncedAt: song.syncedAt,
    upstreamVersion: song.upstreamVersion ?? null,
    primaryImage: song.primaryImage ?? null,
    songType: song.songType,
    year: song.year ?? null,
    durationSeconds: song.durationSeconds ?? null,
    artistCount: numberOrZero(row.artist_count),
    albumCount: numberOrZero(row.album_count),
    tags: song.tags,
  };
}

function mapAlbumSummaryRow(row: AlbumSummaryRow): AlbumSummary {
  const album = parsePayload<Album>(row.payload_json);
  return {
    id: album.id,
    entityType: "album",
    slug: album.slug,
    displayName: album.displayName,
    additionalNames: album.additionalNames,
    url: album.url,
    sourceUrl: album.sourceUrl,
    syncedAt: album.syncedAt,
    upstreamVersion: album.upstreamVersion ?? null,
    primaryImage: album.primaryImage ?? null,
    albumType: album.albumType,
    year: album.year ?? null,
    catalogNumber: album.catalogNumber ?? null,
    trackCount: numberOrZero(row.track_count) || album.tracks.length,
  };
}

function mapAlbumTrackRow(row: AlbumTrackRow): AlbumTrackDetail {
  const song = row.payload_json ? parsePayload<Song>(row.payload_json) : null;
  return {
    trackNumber: row.track_number > 0 ? row.track_number : null,
    discNumber: row.disc_number > 0 ? row.disc_number : null,
    songId: row.song_id,
    name: row.track_name,
    song: song
      ? {
          id: song.id,
          entityType: "song",
          slug: song.slug,
          displayName: song.displayName,
          additionalNames: song.additionalNames,
          url: song.url,
          sourceUrl: song.sourceUrl,
          syncedAt: song.syncedAt,
          upstreamVersion: song.upstreamVersion ?? null,
          primaryImage: song.primaryImage ?? null,
          songType: song.songType,
          year: song.year ?? null,
          durationSeconds: song.durationSeconds ?? null,
          artistCount: numberOrZero(row.artist_count),
          albumCount: numberOrZero(row.album_count),
          tags: song.tags,
        }
      : null,
  };
}

async function queryRows<Row>(query: string, ...params: Array<string | number>) {
  const worker = await getBrowserSqliteWorker();
  if (!worker) {
    throw new Error("Browser SQLite snapshot is unavailable");
  }

  return (await worker.db.query(query, params.length > 0 ? params : undefined)) as Row[];
}

async function safeQueryRows<Row>(query: string, ...params: Array<string | number>): Promise<Row[]> {
  try {
    return await queryRows<Row>(query, ...params);
  } catch {
    return [];
  }
}

async function queryFirst<Row>(query: string, ...params: Array<string | number>) {
  const rows = await queryRows<Row>(query, ...params);
  return rows[0] ?? null;
}

export async function loadDbSnapshotManifest(): Promise<DbSnapshotManifest> {
  if (!manifestPromise) {
    manifestPromise = (async () => {
      let fallbackManifest: DbSnapshotManifest | null = null;

      for (const manifestPath of manifestCandidates) {
        try {
          const response = await fetch(withBase(manifestPath), { cache: "no-store" });
          if (!response.ok) {
            continue;
          }

          const payload = (await response.json()) as Partial<DbSnapshotManifest>;
          const normalized = {
            available: payload.available === true,
            provider: "pages-sqlite" as const,
            version: payload.version ?? null,
            configUrl: payload.configUrl ?? null,
            databaseUrl: payload.databaseUrl ?? null,
            sha256: payload.sha256 ?? null,
            bytes: typeof payload.bytes === "number" ? payload.bytes : null,
            pageSize: typeof payload.pageSize === "number" ? payload.pageSize : null,
            updatedAt: payload.updatedAt ?? null,
          };

          if (normalized.available && normalized.configUrl) {
            return normalized;
          }

          fallbackManifest ??= normalized;
        } catch {
          // Try the next manifest candidate.
        }
      }

      return (
        fallbackManifest ?? {
          available: false,
          provider: "pages-sqlite",
          version: null,
          configUrl: null,
          databaseUrl: null,
          sha256: null,
          bytes: null,
          pageSize: null,
          updatedAt: null,
        }
      );
    })();
  }

  return manifestPromise;
}

async function waitForServiceWorker(timeoutMs = 3000): Promise<void> {
  if (!("serviceWorker" in navigator)) return;
  if (navigator.serviceWorker.controller) return;
  try {
    await Promise.race([
      navigator.serviceWorker.ready,
      new Promise((r) => setTimeout(r, timeoutMs)),
    ]);
  } catch {
    // proceed without SW
  }
}

export async function getBrowserSqliteWorker() {
  if (!workerPromise) {
    workerPromise = (async () => {
      await waitForServiceWorker();

      const manifest = await loadDbSnapshotManifest();
      if (!manifest.available || !manifest.configUrl) {
        return null;
      }

      const config: SplitFileConfig = {
        from: "jsonconfig",
        configUrl: resolveSiteUrl(manifest.configUrl),
      };

      return createDbWorker([config], workerUrl.toString(), wasmUrl.toString(), MAX_BYTES_TO_READ);
    })();
  }

  return workerPromise;
}

export async function getEntityMainBySlug(entityType: "artist", slug: string): Promise<Artist | null>;
export async function getEntityMainBySlug(entityType: "song", slug: string): Promise<Song | null>;
export async function getEntityMainBySlug(entityType: "album", slug: string): Promise<Album | null>;
export async function getEntityMainBySlug(entityType: EntityType, slug: string) {
  const table = entityType === "artist" ? "artists" : entityType === "song" ? "songs" : "albums";
  const row = await queryFirst<PayloadRow>(`SELECT payload_json FROM ${table} WHERE slug = ? LIMIT 1`, slug);
  if (!row) return null;
  return JSON.parse(row.payload_json) as Artist | Song | Album;
}

export async function getEntityDetailBySlug(entityType: "artist", slug: string): Promise<ArtistDetail | null>;
export async function getEntityDetailBySlug(entityType: "song", slug: string): Promise<SongDetail | null>;
export async function getEntityDetailBySlug(entityType: "album", slug: string): Promise<AlbumDetail | null>;
export async function getEntityDetailBySlug(entityType: EntityType, slug: string) {
  if (entityType === "artist") {
    const artistRow = await queryFirst<PayloadRow>("SELECT payload_json FROM artists WHERE slug = ? LIMIT 1", slug);
    if (!artistRow) {
      return null;
    }

    const artist = parsePayload<Artist>(artistRow.payload_json);
    const [relatedArtists, relatedSongs, relatedAlbums] = await Promise.all([
      safeQueryRows<ArtistSummaryRow>(
        `
          SELECT DISTINCT a.payload_json, 0 AS song_count, 0 AS album_count
          FROM artist_relation ar
          JOIN artists a ON a.entity_id = ar.related_artist_id
          WHERE ar.artist_id = ?
          ORDER BY a.display_name ASC, a.slug ASC
        `,
        artist.id,
      ),
      safeQueryRows<SongSummaryRow>(
        `
          SELECT DISTINCT s.payload_json, 0 AS artist_count, 0 AS album_count
          FROM artist_song rel
          JOIN songs s ON s.entity_id = rel.song_id
          WHERE rel.artist_id = ?
          ORDER BY s.display_name ASC, s.slug ASC
        `,
        artist.id,
      ),
      safeQueryRows<AlbumSummaryRow>(
        `
          SELECT DISTINCT a.payload_json, 0 AS track_count
          FROM artist_album rel
          JOIN albums a ON a.entity_id = rel.album_id
          WHERE rel.artist_id = ?
          ORDER BY a.display_name ASC, a.slug ASC
        `,
        artist.id,
      ),
    ]);

    return {
      entityType: "artist",
      entity: artist,
      relatedArtists: relatedArtists.map(mapArtistSummaryRow).filter((entry) => entry.id !== artist.id),
      relatedSongs: relatedSongs.map(mapSongSummaryRow),
      relatedAlbums: relatedAlbums.map(mapAlbumSummaryRow),
    };
  }

  if (entityType === "song") {
    const songRow = await queryFirst<PayloadRow>("SELECT payload_json FROM songs WHERE slug = ? LIMIT 1", slug);
    if (!songRow) {
      return null;
    }

    const song = parsePayload<Song>(songRow.payload_json);
    const [relatedArtists, relatedAlbums] = await Promise.all([
      safeQueryRows<ArtistSummaryRow>(
        `
          SELECT DISTINCT a.payload_json, 0 AS song_count, 0 AS album_count
          FROM artist_song rel
          JOIN artists a ON a.entity_id = rel.artist_id
          WHERE rel.song_id = ?
          ORDER BY a.display_name ASC, a.slug ASC
        `,
        song.id,
      ),
      safeQueryRows<AlbumSummaryRow>(
        `
          SELECT DISTINCT a.payload_json, 0 AS track_count
          FROM album_song rel
          JOIN albums a ON a.entity_id = rel.album_id
          WHERE rel.song_id = ?
          ORDER BY a.display_name ASC, a.slug ASC
        `,
        song.id,
      ),
    ]);

    return {
      entityType: "song",
      entity: song,
      relatedArtists: relatedArtists.map(mapArtistSummaryRow),
      relatedAlbums: relatedAlbums.map(mapAlbumSummaryRow),
    };
  }

  const albumRow = await queryFirst<PayloadRow>("SELECT payload_json FROM albums WHERE slug = ? LIMIT 1", slug);
  if (!albumRow) {
    return null;
  }

  const album = parsePayload<Album>(albumRow.payload_json);
  const [relatedArtists, relatedSongs, tracks] = await Promise.all([
    safeQueryRows<ArtistSummaryRow>(
      `
        SELECT DISTINCT a.payload_json, 0 AS song_count, 0 AS album_count
        FROM artist_album rel
        JOIN artists a ON a.entity_id = rel.artist_id
        WHERE rel.album_id = ?
        ORDER BY a.display_name ASC, a.slug ASC
      `,
      album.id,
    ),
    safeQueryRows<SongSummaryRow>(
      `
        SELECT DISTINCT s.payload_json, 0 AS artist_count, 0 AS album_count
        FROM album_song rel
        JOIN songs s ON s.entity_id = rel.song_id
        WHERE rel.album_id = ?
        ORDER BY s.display_name ASC, s.slug ASC
      `,
      album.id,
    ),
    safeQueryRows<AlbumTrackRow>(
      `
        SELECT
          rel.song_id,
          rel.track_number,
          rel.disc_number,
          rel.track_name,
          s.payload_json,
          0 AS artist_count,
          0 AS album_count
        FROM album_song rel
        LEFT JOIN songs s ON s.entity_id = rel.song_id
        WHERE rel.album_id = ?
        ORDER BY rel.disc_number ASC, rel.track_number ASC, rel.song_id ASC
      `,
      album.id,
    ),
  ]);

  return {
    entityType: "album",
    entity: album,
    relatedArtists: relatedArtists.map(mapArtistSummaryRow),
    relatedSongs: relatedSongs.map(mapSongSummaryRow),
    tracks: tracks.map(mapAlbumTrackRow),
  };
}

export type EntityCountsEntry = {
  songCount?: number;
  albumCount?: number;
  artistCount?: number;
  trackCount?: number;
};

type CountRow = {
  entity_id: string;
  count_a: number;
  count_b: number;
};

export async function getEntityCountsBatch(
  entities: Array<{ entityType: EntityType; id: string }>,
): Promise<Map<string, EntityCountsEntry>> {
  const result = new Map<string, EntityCountsEntry>();
  if (entities.length === 0) return result;

  const byType = new Map<EntityType, string[]>();
  for (const e of entities) {
    let ids = byType.get(e.entityType);
    if (!ids) {
      ids = [];
      byType.set(e.entityType, ids);
    }
    ids.push(e.id);
  }

  const queries: Promise<void>[] = [];

  const artistIds = byType.get("artist");
  if (artistIds && artistIds.length > 0) {
    const placeholders = artistIds.map(() => "?").join(",");
    queries.push(
      safeQueryRows<CountRow>(
        `SELECT a.entity_id,
           COUNT(DISTINCT ars.song_id) AS count_a,
           COUNT(DISTINCT ara.album_id) AS count_b
         FROM artists a
         LEFT JOIN artist_song ars ON ars.artist_id = a.entity_id
         LEFT JOIN artist_album ara ON ara.artist_id = a.entity_id
         WHERE a.entity_id IN (${placeholders})
         GROUP BY a.entity_id`,
        ...artistIds,
      ).then((rows) => {
        for (const r of rows) {
          result.set(r.entity_id, { songCount: r.count_a, albumCount: r.count_b });
        }
      }),
    );
  }

  const songIds = byType.get("song");
  if (songIds && songIds.length > 0) {
    const placeholders = songIds.map(() => "?").join(",");
    queries.push(
      safeQueryRows<CountRow>(
        `SELECT s.entity_id,
           COUNT(DISTINCT ars.artist_id) AS count_a,
           COUNT(DISTINCT als.album_id) AS count_b
         FROM songs s
         LEFT JOIN artist_song ars ON ars.song_id = s.entity_id
         LEFT JOIN album_song als ON als.song_id = s.entity_id
         WHERE s.entity_id IN (${placeholders})
         GROUP BY s.entity_id`,
        ...songIds,
      ).then((rows) => {
        for (const r of rows) {
          result.set(r.entity_id, { artistCount: r.count_a, albumCount: r.count_b });
        }
      }),
    );
  }

  const albumIds = byType.get("album");
  if (albumIds && albumIds.length > 0) {
    const placeholders = albumIds.map(() => "?").join(",");
    queries.push(
      safeQueryRows<CountRow>(
        `SELECT a.entity_id,
           COUNT(DISTINCT als.song_id) AS count_a,
           0 AS count_b
         FROM albums a
         LEFT JOIN album_song als ON als.album_id = a.entity_id
         WHERE a.entity_id IN (${placeholders})
         GROUP BY a.entity_id`,
        ...albumIds,
      ).then((rows) => {
        for (const r of rows) {
          result.set(r.entity_id, { trackCount: r.count_a });
        }
      }),
    );
  }

  await Promise.all(queries);
  return result;
}
