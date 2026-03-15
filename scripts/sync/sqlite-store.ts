import { existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { stableStringify } from "../../src/lib/json";
import { catalogManifestSchema, fullSyncStateSchema } from "../../src/lib/models";
import type {
  Album,
  AlbumDetail,
  AlbumSummary,
  Artist,
  ArtistSummary,
  CatalogScanState,
  EntityRoute,
  EntitySummary,
  EntityType,
  FullSyncState,
  LastRun,
  Seeds,
  Song,
  SongSummary,
} from "../../src/lib/models";
import type { QueueItem } from "../../src/lib/vocadb";
import { initializeSqliteSchema } from "./sqlite-schema";
import type {
  CatalogManifestUpsertRecord,
  FailedEntityRecord,
  KnownEntityRecord,
  LegacyStoreSnapshot,
  MetaStore,
  NormalizedEntity,
  NormalizedStore,
  ReconcileState,
} from "./storage";

type EntityRow = {
  payload_json: string;
};

type KnownRow = {
  vocadb_id: number;
  upstream_version: number | null;
};

type FullSyncStateRow = {
  phase: "scan" | "hydrate";
  entity_type: EntityType;
  next_start: number;
  page_size: number;
  pass: number;
  run_token: string;
  pages_fetched: number;
  unique_ids_seen: number;
  duplicate_ids_seen: number;
  hydrated_count: number;
  updated_at: string;
};

type ArtistSongRelationRow = {
  artist_id: string;
  song_id: string;
};

type ArtistAlbumRelationRow = {
  artist_id: string;
  album_id: string;
};

type AlbumSongRelationRow = {
  album_id: string;
  song_id: string;
  track_number: number;
  disc_number: number;
  track_name: string;
};

type ArtistRelationRow = {
  artist_id: string;
  related_artist_id: string;
  relation_kind: string;
};

type HotEntityRow = {
  entity_type: EntityType;
  vocadb_id: number;
  upstream_version: number | null;
};


function entityTable(entityType: EntityType) {
  if (entityType === "artist") {
    return "artists";
  }
  if (entityType === "song") {
    return "songs";
  }
  return "albums";
}

function entityYear(entity: NormalizedEntity) {
  return "year" in entity && typeof entity.year === "number" ? entity.year : null;
}

function uniqueSorted(values: string[]) {
  return [...new Set(values)].sort((left, right) => left.localeCompare(right));
}

function parseEntityPayload<T>(payloadJson: string): T {
  return JSON.parse(payloadJson) as T;
}

const BULK_BATCH_SIZE = 800;

function chunked<T>(items: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

export class SqliteSyncStore implements NormalizedStore, MetaStore {
  private readonly db: Database.Database;
  private readonly dryRun: boolean;

  constructor(databasePath: string, dryRun: boolean) {
    const targetPath = dryRun && !existsSync(databasePath) ? ":memory:" : databasePath;

    if (targetPath !== ":memory:") {
      mkdirSync(path.dirname(databasePath), { recursive: true });
    }

    this.db = new Database(targetPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    initializeSqliteSchema(this.db);
    this.dryRun = dryRun;
  }

  async hasAnyEntities() {
    const row = this.db
      .prepare<unknown[], { has_entities: number }>(`
        SELECT
          CASE
            WHEN EXISTS(SELECT 1 FROM artists LIMIT 1) THEN 1
            WHEN EXISTS(SELECT 1 FROM songs LIMIT 1) THEN 1
            WHEN EXISTS(SELECT 1 FROM albums LIMIT 1) THEN 1
            ELSE 0
          END AS has_entities
      `)
      .get();
    return row?.has_entities === 1;
  }

  async importLegacySnapshot(snapshot: LegacyStoreSnapshot) {
    if (this.dryRun || (await this.hasAnyEntities())) {
      return;
    }

    const importTransaction = this.db.transaction((legacy: LegacyStoreSnapshot) => {
      for (const artist of legacy.artists) {
        this.persistEntityUnchecked(artist);
      }
      for (const song of legacy.songs) {
        this.persistEntityUnchecked(song);
      }
      for (const album of legacy.albums) {
        this.persistEntityUnchecked(album);
      }

      this.replaceFailedEntities(legacy.failedEntities);
      this.saveSetting("seeds_snapshot", legacy.seeds);

      if (legacy.lastRun) {
        this.insertSyncRun(legacy.lastRun);
        this.saveSetting("last_run", legacy.lastRun);
      }
    });

    importTransaction(snapshot);
  }

  async listEntities<T extends NormalizedEntity>(entityType: EntityType): Promise<T[]> {
    const table = entityTable(entityType);
    const rows = this.db
      .prepare<unknown[], EntityRow>(`SELECT payload_json FROM ${table} ORDER BY slug ASC`)
      .all();
    return rows.map((row) => parseEntityPayload<T>(row.payload_json));
  }

  *iterateEntities<T extends NormalizedEntity>(entityType: EntityType): Iterable<T> {
    const table = entityTable(entityType);
    const statement = this.db.prepare<unknown[], EntityRow>(`SELECT payload_json FROM ${table} ORDER BY slug ASC`);

    for (const row of statement.iterate()) {
      yield parseEntityPayload<T>(row.payload_json);
    }
  }

  *iterateArtistSummaries(): Iterable<ArtistSummary> {
    const statement = this.db.prepare<
      unknown[],
      EntityRow & { song_count: number; album_count: number }
    >(
      `
        SELECT
          a.payload_json,
          (SELECT COUNT(*) FROM artist_song WHERE artist_id = a.entity_id) AS song_count,
          (SELECT COUNT(*) FROM artist_album WHERE artist_id = a.entity_id) AS album_count
        FROM artists a
        ORDER BY a.display_name ASC, a.slug ASC
      `,
    );

    for (const row of statement.iterate()) {
      const artist = parseEntityPayload<Artist>(row.payload_json);
      yield {
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
        songCount: row.song_count,
        albumCount: row.album_count,
      };
    }
  }

  *iterateSongSummaries(): Iterable<SongSummary> {
    const statement = this.db.prepare<
      unknown[],
      EntityRow & { artist_count: number; album_count: number }
    >(
      `
        SELECT
          s.payload_json,
          (SELECT COUNT(*) FROM artist_song WHERE song_id = s.entity_id) AS artist_count,
          (SELECT COUNT(*) FROM album_song WHERE song_id = s.entity_id) AS album_count
        FROM songs s
        ORDER BY s.display_name ASC, s.slug ASC
      `,
    );

    for (const row of statement.iterate()) {
      const song = parseEntityPayload<Song>(row.payload_json);
      yield {
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
        artistCount: row.artist_count,
        albumCount: row.album_count,
        tags: song.tags,
      };
    }
  }

  *iterateAlbumSummaries(): Iterable<AlbumSummary> {
    const statement = this.db.prepare<
      unknown[],
      EntityRow & { track_count: number }
    >(
      `
        SELECT
          a.payload_json,
          (SELECT COUNT(*) FROM album_song WHERE album_id = a.entity_id) AS track_count
        FROM albums a
        ORDER BY a.display_name ASC, a.slug ASC
      `,
    );

    for (const row of statement.iterate()) {
      const album = parseEntityPayload<Album>(row.payload_json);
      yield {
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
        trackCount: row.track_count || album.tracks.length,
      };
    }
  }

  async getEntityById<T extends NormalizedEntity>(entityType: EntityType, entityId: string): Promise<T | null> {
    const table = entityTable(entityType);
    const row = this.db.prepare<[string], EntityRow>(`SELECT payload_json FROM ${table} WHERE entity_id = ?`).get(entityId);
    return row ? parseEntityPayload<T>(row.payload_json) : null;
  }

  async countEntities(entityType: EntityType): Promise<number> {
    const table = entityTable(entityType);
    return this.db.prepare<unknown[], { value: number }>(`SELECT COUNT(*) AS value FROM ${table}`).get()?.value ?? 0;
  }

  async listChangedEntitiesSince(entityType: EntityType, syncedAfter: string) {
    const table = entityTable(entityType);
    return this.db
      .prepare<[string], { entity_id: string; slug: string; display_name: string }>(
        `
          SELECT entity_id, slug, display_name
          FROM ${table}
          WHERE synced_at > ?
          ORDER BY synced_at ASC, entity_id ASC
        `,
      )
      .all(syncedAfter)
      .map((row) => ({
        entityId: row.entity_id,
        slug: row.slug,
        displayName: row.display_name,
      }));
  }

  async listSummaryPage(entityType: EntityType, pageNumber: number, pageSize: number) {
    const limit = Math.max(pageSize, 1);
    const offset = Math.max(pageNumber - 1, 0) * limit;

    if (entityType === "artist") {
      return this.listArtistSummariesForQuery(
        `
          SELECT a.payload_json
          FROM artists a
          LEFT JOIN catalog_manifest cm ON cm.entity_type = 'artist' AND cm.vocadb_id = a.vocadb_id
          ORDER BY COALESCE(cm.created_at, a.synced_at) DESC, a.entity_id ASC
          LIMIT ? OFFSET ?
        `,
        limit,
        offset,
      );
    }

    if (entityType === "song") {
      return this.listSongSummariesForQuery(
        `
          SELECT s.payload_json
          FROM songs s
          LEFT JOIN catalog_manifest cm ON cm.entity_type = 'song' AND cm.vocadb_id = s.vocadb_id
          ORDER BY COALESCE(cm.created_at, s.synced_at) DESC, s.entity_id ASC
          LIMIT ? OFFSET ?
        `,
        limit,
        offset,
      );
    }

    return this.listAlbumSummariesForQuery(
      `
        SELECT a.payload_json
        FROM albums a
        LEFT JOIN catalog_manifest cm ON cm.entity_type = 'album' AND cm.vocadb_id = a.vocadb_id
        ORDER BY COALESCE(cm.created_at, a.synced_at) DESC, a.entity_id ASC
        LIMIT ? OFFSET ?
      `,
      limit,
      offset,
    );
  }

  async listRouteItems(entityType: EntityType): Promise<EntityRoute[]> {
    const table = entityTable(entityType);
    const rows = this.db
      .prepare<unknown[], { id: string; slug: string; display_name: string }>(
        `
          SELECT t.entity_id AS id, t.slug, t.display_name
          FROM ${table} t
          LEFT JOIN catalog_manifest cm ON cm.entity_type = ? AND cm.vocadb_id = t.vocadb_id
          ORDER BY COALESCE(cm.created_at, t.synced_at) DESC, t.entity_id ASC
        `,
      )
      .all(entityType);

    return rows.map((row) => ({
      id: row.id,
      slug: row.slug,
      displayName: row.display_name,
      pageNumber: 0,
    }));
  }

  async getEntitySummaryById(entityId: string): Promise<EntitySummary | null> {
    const entityType = entityId.startsWith("artist-")
      ? "artist"
      : entityId.startsWith("song-")
        ? "song"
        : entityId.startsWith("album-")
          ? "album"
          : null;

    if (!entityType) {
      return null;
    }

    const summaries = await this.listSummaryPageForEntityId(entityType, entityId);
    return summaries[0] ?? null;
  }

  async listRecentSummaries(limit: number): Promise<EntitySummary[]> {
    const perType = Math.max(limit, 1);
    const artistRows = this.db
      .prepare<unknown[], EntityRow & { song_count: number; album_count: number }>(
        `SELECT a.payload_json,
          (SELECT COUNT(*) FROM artist_song WHERE artist_id = a.entity_id) AS song_count,
          (SELECT COUNT(*) FROM artist_album WHERE artist_id = a.entity_id) AS album_count
        FROM artists a ORDER BY a.synced_at DESC, a.entity_id ASC LIMIT ?`,
      )
      .all(perType);
    const songRows = this.db
      .prepare<unknown[], EntityRow & { artist_count: number; album_count: number }>(
        `SELECT s.payload_json,
          (SELECT COUNT(*) FROM artist_song WHERE song_id = s.entity_id) AS artist_count,
          (SELECT COUNT(*) FROM album_song WHERE song_id = s.entity_id) AS album_count
        FROM songs s ORDER BY s.synced_at DESC, s.entity_id ASC LIMIT ?`,
      )
      .all(perType);
    const albumRows = this.db
      .prepare<unknown[], EntityRow & { track_count: number }>(
        `SELECT al.payload_json,
          (SELECT COUNT(*) FROM album_song WHERE album_id = al.entity_id) AS track_count
        FROM albums al ORDER BY al.synced_at DESC, al.entity_id ASC LIMIT ?`,
      )
      .all(perType);

    const combined: Array<{ syncedAt: string; entityId: string; summary: EntitySummary }> = [];

    for (const row of artistRows) {
      const artist = parseEntityPayload<Artist>(row.payload_json);
      combined.push({
        syncedAt: artist.syncedAt,
        entityId: artist.id,
        summary: {
          id: artist.id, entityType: "artist", slug: artist.slug, displayName: artist.displayName,
          additionalNames: artist.additionalNames, url: artist.url, sourceUrl: artist.sourceUrl,
          syncedAt: artist.syncedAt, upstreamVersion: artist.upstreamVersion ?? null,
          primaryImage: artist.primaryImage ?? null, artistType: artist.artistType,
          descriptionShort: artist.descriptionShort, songCount: row.song_count, albumCount: row.album_count,
        },
      });
    }
    for (const row of songRows) {
      const song = parseEntityPayload<Song>(row.payload_json);
      combined.push({
        syncedAt: song.syncedAt,
        entityId: song.id,
        summary: {
          id: song.id, entityType: "song", slug: song.slug, displayName: song.displayName,
          additionalNames: song.additionalNames, url: song.url, sourceUrl: song.sourceUrl,
          syncedAt: song.syncedAt, upstreamVersion: song.upstreamVersion ?? null,
          primaryImage: song.primaryImage ?? null, songType: song.songType,
          year: song.year ?? null, durationSeconds: song.durationSeconds ?? null,
          artistCount: row.artist_count, albumCount: row.album_count, tags: song.tags,
        },
      });
    }
    for (const row of albumRows) {
      const album = parseEntityPayload<Album>(row.payload_json);
      combined.push({
        syncedAt: album.syncedAt,
        entityId: album.id,
        summary: {
          id: album.id, entityType: "album", slug: album.slug, displayName: album.displayName,
          additionalNames: album.additionalNames, url: album.url, sourceUrl: album.sourceUrl,
          syncedAt: album.syncedAt, upstreamVersion: album.upstreamVersion ?? null,
          primaryImage: album.primaryImage ?? null, albumType: album.albumType,
          year: album.year ?? null, catalogNumber: album.catalogNumber ?? null,
          trackCount: row.track_count || album.tracks.length,
        },
      });
    }

    combined.sort((a, b) => b.syncedAt.localeCompare(a.syncedAt) || a.entityId.localeCompare(b.entityId));
    return combined.slice(0, limit).map((entry) => entry.summary);
  }

  async listUpdatedTodaySummaries(dayIso: string): Promise<EntitySummary[]> {
    const nextDay = new Date(`${dayIso}T00:00:00.000Z`);
    nextDay.setUTCDate(nextDay.getUTCDate() + 1);
    const nextDayIso = nextDay.toISOString().slice(0, 10);
    const dayStart = `${dayIso}T00:00:00.000Z`;
    const dayEnd = `${nextDayIso}T00:00:00.000Z`;

    const artistRows = this.db
      .prepare<unknown[], EntityRow & { song_count: number; album_count: number }>(
        `SELECT a.payload_json,
          (SELECT COUNT(*) FROM artist_song WHERE artist_id = a.entity_id) AS song_count,
          (SELECT COUNT(*) FROM artist_album WHERE artist_id = a.entity_id) AS album_count
        FROM artists a WHERE a.synced_at >= ? AND a.synced_at < ?
        ORDER BY a.synced_at DESC, a.entity_id ASC`,
      )
      .all(dayStart, dayEnd);
    const songRows = this.db
      .prepare<unknown[], EntityRow & { artist_count: number; album_count: number }>(
        `SELECT s.payload_json,
          (SELECT COUNT(*) FROM artist_song WHERE song_id = s.entity_id) AS artist_count,
          (SELECT COUNT(*) FROM album_song WHERE song_id = s.entity_id) AS album_count
        FROM songs s WHERE s.synced_at >= ? AND s.synced_at < ?
        ORDER BY s.synced_at DESC, s.entity_id ASC`,
      )
      .all(dayStart, dayEnd);
    const albumRows = this.db
      .prepare<unknown[], EntityRow & { track_count: number }>(
        `SELECT al.payload_json,
          (SELECT COUNT(*) FROM album_song WHERE album_id = al.entity_id) AS track_count
        FROM albums al WHERE al.synced_at >= ? AND al.synced_at < ?
        ORDER BY al.synced_at DESC, al.entity_id ASC`,
      )
      .all(dayStart, dayEnd);

    const combined: Array<{ syncedAt: string; entityId: string; summary: EntitySummary }> = [];

    for (const row of artistRows) {
      const artist = parseEntityPayload<Artist>(row.payload_json);
      combined.push({
        syncedAt: artist.syncedAt, entityId: artist.id,
        summary: {
          id: artist.id, entityType: "artist", slug: artist.slug, displayName: artist.displayName,
          additionalNames: artist.additionalNames, url: artist.url, sourceUrl: artist.sourceUrl,
          syncedAt: artist.syncedAt, upstreamVersion: artist.upstreamVersion ?? null,
          primaryImage: artist.primaryImage ?? null, artistType: artist.artistType,
          descriptionShort: artist.descriptionShort, songCount: row.song_count, albumCount: row.album_count,
        },
      });
    }
    for (const row of songRows) {
      const song = parseEntityPayload<Song>(row.payload_json);
      combined.push({
        syncedAt: song.syncedAt, entityId: song.id,
        summary: {
          id: song.id, entityType: "song", slug: song.slug, displayName: song.displayName,
          additionalNames: song.additionalNames, url: song.url, sourceUrl: song.sourceUrl,
          syncedAt: song.syncedAt, upstreamVersion: song.upstreamVersion ?? null,
          primaryImage: song.primaryImage ?? null, songType: song.songType,
          year: song.year ?? null, durationSeconds: song.durationSeconds ?? null,
          artistCount: row.artist_count, albumCount: row.album_count, tags: song.tags,
        },
      });
    }
    for (const row of albumRows) {
      const album = parseEntityPayload<Album>(row.payload_json);
      combined.push({
        syncedAt: album.syncedAt, entityId: album.id,
        summary: {
          id: album.id, entityType: "album", slug: album.slug, displayName: album.displayName,
          additionalNames: album.additionalNames, url: album.url, sourceUrl: album.sourceUrl,
          syncedAt: album.syncedAt, upstreamVersion: album.upstreamVersion ?? null,
          primaryImage: album.primaryImage ?? null, albumType: album.albumType,
          year: album.year ?? null, catalogNumber: album.catalogNumber ?? null,
          trackCount: row.track_count || album.tracks.length,
        },
      });
    }

    combined.sort((a, b) => b.syncedAt.localeCompare(a.syncedAt) || a.entityId.localeCompare(b.entityId));
    return combined.map((entry) => entry.summary);
  }

  async listRelatedArtistSummaries(artistId: string): Promise<ArtistSummary[]> {
    return this.listArtistSummariesForQuery(
      `
        SELECT DISTINCT a.payload_json
        FROM artist_relation ar
        JOIN artists a ON a.entity_id = ar.related_artist_id
        WHERE ar.artist_id = ?
        ORDER BY a.display_name ASC, a.slug ASC
      `,
      artistId,
    );
  }

  async listArtistSongSummaries(artistId: string): Promise<SongSummary[]> {
    return this.listSongSummariesForQuery(
      `
        SELECT DISTINCT s.payload_json
        FROM artist_song rel
        JOIN songs s ON s.entity_id = rel.song_id
        WHERE rel.artist_id = ?
        ORDER BY s.display_name ASC, s.slug ASC
      `,
      artistId,
    );
  }

  async listArtistSongIds(artistId: string): Promise<string[]> {
    return this.db
      .prepare<[string], { song_id: string }>("SELECT song_id FROM artist_song WHERE artist_id = ? ORDER BY song_id ASC")
      .all(artistId)
      .map((row) => row.song_id);
  }

  async listArtistAlbumSummaries(artistId: string): Promise<AlbumSummary[]> {
    return this.listAlbumSummariesForQuery(
      `
        SELECT DISTINCT a.payload_json
        FROM artist_album rel
        JOIN albums a ON a.entity_id = rel.album_id
        WHERE rel.artist_id = ?
        ORDER BY a.display_name ASC, a.slug ASC
      `,
      artistId,
    );
  }

  async listArtistAlbumIds(artistId: string): Promise<string[]> {
    return this.db
      .prepare<[string], { album_id: string }>("SELECT album_id FROM artist_album WHERE artist_id = ? ORDER BY album_id ASC")
      .all(artistId)
      .map((row) => row.album_id);
  }

  async listSongArtistSummaries(songId: string): Promise<ArtistSummary[]> {
    return this.listArtistSummariesForQuery(
      `
        SELECT DISTINCT a.payload_json
        FROM artist_song rel
        JOIN artists a ON a.entity_id = rel.artist_id
        WHERE rel.song_id = ?
        ORDER BY a.display_name ASC, a.slug ASC
      `,
      songId,
    );
  }

  async listSongArtistIds(songId: string): Promise<string[]> {
    return this.db
      .prepare<[string], { artist_id: string }>("SELECT artist_id FROM artist_song WHERE song_id = ? ORDER BY artist_id ASC")
      .all(songId)
      .map((row) => row.artist_id);
  }

  async listSongAlbumSummaries(songId: string): Promise<AlbumSummary[]> {
    return this.listAlbumSummariesForQuery(
      `
        SELECT DISTINCT a.payload_json
        FROM album_song rel
        JOIN albums a ON a.entity_id = rel.album_id
        WHERE rel.song_id = ?
        ORDER BY a.display_name ASC, a.slug ASC
      `,
      songId,
    );
  }

  async listSongAlbumIds(songId: string): Promise<string[]> {
    return this.db
      .prepare<[string], { album_id: string }>("SELECT album_id FROM album_song WHERE song_id = ? ORDER BY album_id ASC")
      .all(songId)
      .map((row) => row.album_id);
  }

  async listAlbumArtistSummaries(albumId: string): Promise<ArtistSummary[]> {
    return this.listArtistSummariesForQuery(
      `
        SELECT DISTINCT a.payload_json
        FROM artist_album rel
        JOIN artists a ON a.entity_id = rel.artist_id
        WHERE rel.album_id = ?
        ORDER BY a.display_name ASC, a.slug ASC
      `,
      albumId,
    );
  }

  async listAlbumArtistIds(albumId: string): Promise<string[]> {
    return this.db
      .prepare<[string], { artist_id: string }>("SELECT artist_id FROM artist_album WHERE album_id = ? ORDER BY artist_id ASC")
      .all(albumId)
      .map((row) => row.artist_id);
  }

  async listAlbumSongSummaries(albumId: string): Promise<SongSummary[]> {
    return this.listSongSummariesForQuery(
      `
        SELECT DISTINCT s.payload_json
        FROM album_song rel
        JOIN songs s ON s.entity_id = rel.song_id
        WHERE rel.album_id = ?
        ORDER BY s.display_name ASC, s.slug ASC
      `,
      albumId,
    );
  }

  async listAlbumSongIds(albumId: string): Promise<string[]> {
    return this.db
      .prepare<[string], { song_id: string }>("SELECT song_id FROM album_song WHERE album_id = ? ORDER BY song_id ASC")
      .all(albumId)
      .map((row) => row.song_id);
  }

  async listArtistRelatedIds(artistId: string): Promise<string[]> {
    const rows = this.db
      .prepare<[string, string], { related_artist_id: string }>(
        `
          SELECT DISTINCT related_artist_id
          FROM (
            SELECT related_artist_id
            FROM artist_relation
            WHERE artist_id = ?
            UNION
            SELECT artist_id AS related_artist_id
            FROM artist_relation
            WHERE related_artist_id = ?
          )
          ORDER BY related_artist_id ASC
        `,
      )
      .all(artistId, artistId);

    return rows.map((row) => row.related_artist_id).filter((id) => id !== artistId);
  }

  async listAlbumTracks(albumId: string): Promise<AlbumDetail["tracks"]> {
    const rows = this.db
      .prepare<[string], { song_id: string; track_number: number; disc_number: number; track_name: string; payload_json: string | null }>(
        `
          SELECT rel.song_id, rel.track_number, rel.disc_number, rel.track_name, s.payload_json
          FROM album_song rel
          LEFT JOIN songs s ON s.entity_id = rel.song_id
          WHERE rel.album_id = ?
          ORDER BY rel.disc_number ASC, rel.track_number ASC, rel.song_id ASC
        `,
      )
      .all(albumId);

    const songIds = rows.filter((r) => r.payload_json !== null).map((r) => r.song_id);
    const counts = this.bulkSongCounts(songIds);

    return rows.map((row) => {
      const song = row.payload_json ? parseEntityPayload<Song>(row.payload_json) : null;
      const c = song ? counts.get(song.id) : null;
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
              artistCount: c?.artistCount ?? 0,
              albumCount: c?.albumCount ?? 0,
              tags: song.tags,
            }
          : null,
      };
    });
  }

  async getKnownEntities(entityType: EntityType): Promise<KnownEntityRecord[]> {
    return this.getKnownEntitiesPage(entityType);
  }

  async getKnownEntitiesPage(
    entityType: EntityType,
    options?: {
      afterVocadbId?: number;
      limit?: number;
      bucket?: number;
      bucketCount?: number;
    },
  ): Promise<KnownEntityRecord[]> {
    const table = entityTable(entityType);
    const afterVocadbId = options?.afterVocadbId ?? 0;
    const limit = options?.limit ?? Number.MAX_SAFE_INTEGER;
    const useBucket = options?.bucket != null && options?.bucketCount != null && options.bucketCount > 1;
    const rows = useBucket
      ? this.db
          .prepare<[number, number, number, number], KnownRow>(
            `
              SELECT vocadb_id, upstream_version
              FROM ${table}
              WHERE vocadb_id > ?
                AND (vocadb_id % ?) = ?
              ORDER BY vocadb_id ASC
              LIMIT ?
            `,
          )
          .all(afterVocadbId, options!.bucketCount!, options!.bucket!, limit)
      : this.db
          .prepare<[number, number], KnownRow>(
            `
              SELECT vocadb_id, upstream_version
              FROM ${table}
              WHERE vocadb_id > ?
              ORDER BY vocadb_id ASC
              LIMIT ?
            `,
          )
          .all(afterVocadbId, limit);
    return rows.map((row) => ({
      vocadbId: row.vocadb_id,
      upstreamVersion: row.upstream_version ?? null,
    }));
  }

  getKnownVersion(entityType: EntityType, vocadbId: number): number | null {
    const table = entityTable(entityType);
    const row = this.db
      .prepare<[number], KnownRow | undefined>(
        `SELECT vocadb_id, upstream_version FROM ${table} WHERE vocadb_id = ? LIMIT 1`,
      )
      .get(vocadbId);
    return row?.upstream_version ?? null;
  }

  async getKnownVersions(entityType: EntityType, vocadbIds: number[]): Promise<Map<number, number | null>> {
    if (vocadbIds.length === 0) {
      return new Map();
    }

    const table = entityTable(entityType);
    const placeholders = vocadbIds.map(() => "?").join(", ");
    const rows = this.db
      .prepare<unknown[], KnownRow>(
        `
          SELECT vocadb_id, upstream_version
          FROM ${table}
          WHERE vocadb_id IN (${placeholders})
        `,
      )
      .all(...vocadbIds);

    return new Map(rows.map((row) => [row.vocadb_id, row.upstream_version ?? null]));
  }

  async getHotEntities(limit: number): Promise<Array<QueueItem & { knownVersion: number | null }>> {
    const rows = this.db
      .prepare<unknown[], HotEntityRow>(
        `
          SELECT entity_type, vocadb_id, upstream_version
          FROM (
            SELECT 'artist' AS entity_type, vocadb_id, upstream_version, synced_at FROM artists
            UNION ALL
            SELECT 'song' AS entity_type, vocadb_id, upstream_version, synced_at FROM songs
            UNION ALL
            SELECT 'album' AS entity_type, vocadb_id, upstream_version, synced_at FROM albums
          )
          ORDER BY synced_at DESC, vocadb_id DESC
          LIMIT ?
        `,
      )
      .all(limit);

    return rows.map((row) => ({
      entityType: row.entity_type,
      vocadbId: row.vocadb_id,
      depth: 1,
      discoveredFrom: "sqlite-hotset",
      reason: "hot-entity-refresh",
      knownVersion: row.upstream_version ?? null,
    }));
  }

  async loadReconcileState(): Promise<ReconcileState | null> {
    return this.loadSetting<ReconcileState>("reconcile_state");
  }

  async saveReconcileState(state: ReconcileState) {
    if (this.dryRun) {
      return;
    }

    this.saveSetting("reconcile_state", state);
  }

  async loadCatalogScanState(): Promise<CatalogScanState | null> {
    return this.loadSetting<CatalogScanState>("catalog_scan_state");
  }

  async saveCatalogScanState(state: CatalogScanState) {
    if (this.dryRun) {
      return;
    }

    this.saveSetting("catalog_scan_state", state);
  }

  async clearCatalogScanState() {
    if (this.dryRun) {
      return;
    }

    this.deleteSetting("catalog_scan_state");
  }

  async loadFullSyncState(): Promise<FullSyncState | null> {
    const row = this.db
      .prepare<[string], FullSyncStateRow>(
        `
          SELECT
            phase,
            entity_type,
            next_start,
            page_size,
            pass,
            run_token,
            pages_fetched,
            unique_ids_seen,
            duplicate_ids_seen,
            hydrated_count,
            updated_at
          FROM full_sync_state
          WHERE state_key = ?
        `,
      )
      .get("current");
    if (!row) {
      return null;
    }

    return fullSyncStateSchema.parse({
      phase: row.phase,
      entityType: row.entity_type,
      nextStart: row.next_start,
      pageSize: row.page_size,
      pass: row.pass,
      runToken: row.run_token,
      pagesFetched: row.pages_fetched,
      uniqueIdsSeen: row.unique_ids_seen,
      duplicateIdsSeen: row.duplicate_ids_seen,
      hydratedCount: row.hydrated_count,
      updatedAt: row.updated_at,
    });
  }

  async saveFullSyncState(state: FullSyncState) {
    if (this.dryRun) {
      return;
    }

    this.db
      .prepare(
        `
          INSERT INTO full_sync_state (
            state_key,
            phase,
            entity_type,
            next_start,
            page_size,
            pass,
            run_token,
            pages_fetched,
            unique_ids_seen,
            duplicate_ids_seen,
            hydrated_count,
            updated_at
          )
          VALUES (
            @state_key,
            @phase,
            @entity_type,
            @next_start,
            @page_size,
            @pass,
            @run_token,
            @pages_fetched,
            @unique_ids_seen,
            @duplicate_ids_seen,
            @hydrated_count,
            @updated_at
          )
          ON CONFLICT(state_key) DO UPDATE SET
            phase = excluded.phase,
            entity_type = excluded.entity_type,
            next_start = excluded.next_start,
            page_size = excluded.page_size,
            pass = excluded.pass,
            run_token = excluded.run_token,
            pages_fetched = excluded.pages_fetched,
            unique_ids_seen = excluded.unique_ids_seen,
            duplicate_ids_seen = excluded.duplicate_ids_seen,
            hydrated_count = excluded.hydrated_count,
            updated_at = excluded.updated_at
        `,
      )
      .run({
        state_key: "current",
        phase: state.phase,
        entity_type: state.entityType,
        next_start: state.nextStart,
        page_size: state.pageSize,
        pass: state.pass,
        run_token: state.runToken,
        pages_fetched: state.pagesFetched,
        unique_ids_seen: state.uniqueIdsSeen,
        duplicate_ids_seen: state.duplicateIdsSeen,
        hydrated_count: state.hydratedCount,
        updated_at: state.updatedAt,
      });
  }

  async clearFullSyncState() {
    if (this.dryRun) {
      return;
    }

    this.db.prepare("DELETE FROM full_sync_state WHERE state_key = ?").run("current");
  }

  async upsertCatalogManifestEntries(entries: CatalogManifestUpsertRecord[]) {
    if (this.dryRun || entries.length === 0) {
      return;
    }

    const now = new Date().toISOString();
    const statement = this.db.prepare(
      `
        INSERT INTO catalog_manifest (
          entity_type,
          vocadb_id,
          display_name,
          upstream_version,
          published_at,
          created_at,
          release_date,
          last_seen_run_token,
          needs_hydrate,
          hydrate_state,
          hydrated_at,
          last_error,
          updated_at
        )
        VALUES (
          @entity_type,
          @vocadb_id,
          @display_name,
          @upstream_version,
          @published_at,
          @created_at,
          @release_date,
          @last_seen_run_token,
          @needs_hydrate,
          @hydrate_state,
          @hydrated_at,
          @last_error,
          @updated_at
        )
        ON CONFLICT(entity_type, vocadb_id) DO UPDATE SET
          display_name = excluded.display_name,
          upstream_version = excluded.upstream_version,
          published_at = excluded.published_at,
          created_at = excluded.created_at,
          release_date = excluded.release_date,
          last_seen_run_token = excluded.last_seen_run_token,
          needs_hydrate = excluded.needs_hydrate,
          hydrate_state = CASE
            WHEN excluded.needs_hydrate = 0 THEN 'hydrated'
            WHEN catalog_manifest.hydrate_state = 'failed' THEN 'failed'
            ELSE 'pending'
          END,
          hydrated_at = CASE
            WHEN excluded.needs_hydrate = 0 THEN COALESCE(catalog_manifest.hydrated_at, excluded.updated_at)
            ELSE catalog_manifest.hydrated_at
          END,
          last_error = CASE
            WHEN excluded.needs_hydrate = 0 THEN NULL
            ELSE catalog_manifest.last_error
          END,
          updated_at = excluded.updated_at
      `,
    );

    this.db.transaction((items: CatalogManifestUpsertRecord[]) => {
      for (const entry of items) {
        const manifest = catalogManifestSchema.parse({
          ...entry,
          upstreamVersion: entry.upstreamVersion ?? null,
          publishedAt: entry.publishedAt ?? null,
          createdAt: entry.createdAt ?? null,
          releaseDate: entry.releaseDate ?? null,
          hydrateState: entry.needsHydrate ? "pending" : "hydrated",
          hydratedAt: entry.needsHydrate ? null : now,
          lastError: null,
          updatedAt: now,
        });

        statement.run({
          entity_type: manifest.entityType,
          vocadb_id: manifest.vocadbId,
          display_name: manifest.displayName,
          upstream_version: manifest.upstreamVersion ?? null,
          published_at: manifest.publishedAt ?? null,
          created_at: manifest.createdAt ?? null,
          release_date: manifest.releaseDate ?? null,
          last_seen_run_token: manifest.lastSeenRunToken,
          needs_hydrate: manifest.needsHydrate ? 1 : 0,
          hydrate_state: manifest.hydrateState,
          hydrated_at: manifest.hydratedAt ?? null,
          last_error: manifest.lastError ?? null,
          updated_at: manifest.updatedAt,
        });
      }
    })(entries);
  }

  async listHydrateCandidates(entityType: EntityType, afterVocadbId: number, limit: number): Promise<KnownEntityRecord[]> {
    const rows = this.db
      .prepare<[EntityType, number, number], KnownRow>(
        `
          SELECT vocadb_id, upstream_version
          FROM catalog_manifest
          WHERE entity_type = ?
            AND needs_hydrate = 1
            AND vocadb_id > ?
          ORDER BY vocadb_id ASC
          LIMIT ?
        `,
      )
      .all(entityType, Math.max(afterVocadbId, 0), Math.max(limit, 1));

    return rows.map((row) => ({
      vocadbId: row.vocadb_id,
      upstreamVersion: row.upstream_version ?? null,
    }));
  }

  async markManifestHydrated(entityType: EntityType, vocadbId: number) {
    if (this.dryRun) {
      return;
    }

    const updatedAt = new Date().toISOString();
    this.db
      .prepare<[string, string, EntityType, number]>(
        `
          UPDATE catalog_manifest
          SET
            needs_hydrate = 0,
            hydrate_state = 'hydrated',
            hydrated_at = ?,
            last_error = NULL,
            updated_at = ?
          WHERE entity_type = ?
            AND vocadb_id = ?
        `,
      )
      .run(updatedAt, updatedAt, entityType, vocadbId);
  }

  async markManifestHydrateFailed(entityType: EntityType, vocadbId: number, error: string) {
    if (this.dryRun) {
      return;
    }

    const updatedAt = new Date().toISOString();
    this.db
      .prepare<[string, string, EntityType, number]>(
        `
          UPDATE catalog_manifest
          SET
            needs_hydrate = 1,
            hydrate_state = 'failed',
            last_error = ?,
            updated_at = ?
          WHERE entity_type = ?
            AND vocadb_id = ?
        `,
      )
      .run(error, updatedAt, entityType, vocadbId);
  }

  async countPendingHydrates(): Promise<number> {
    return this.db
      .prepare<unknown[], { value: number }>("SELECT COUNT(*) AS value FROM catalog_manifest WHERE needs_hydrate = 1")
      .get()?.value ?? 0;
  }

  async listArtistSongRelations() {
    return this.db
      .prepare<unknown[], ArtistSongRelationRow>("SELECT artist_id, song_id FROM artist_song ORDER BY artist_id ASC, song_id ASC")
      .all()
      .map((row) => ({
        artistId: row.artist_id,
        songId: row.song_id,
      }));
  }

  *iterateArtistSongRelations() {
    const statement = this.db.prepare<unknown[], ArtistSongRelationRow>(
      "SELECT artist_id, song_id FROM artist_song ORDER BY artist_id ASC, song_id ASC",
    );

    for (const row of statement.iterate()) {
      yield {
        artistId: row.artist_id,
        songId: row.song_id,
      };
    }
  }

  async listArtistAlbumRelations() {
    return this.db
      .prepare<unknown[], ArtistAlbumRelationRow>(
        "SELECT artist_id, album_id FROM artist_album ORDER BY artist_id ASC, album_id ASC",
      )
      .all()
      .map((row) => ({
        artistId: row.artist_id,
        albumId: row.album_id,
      }));
  }

  *iterateArtistAlbumRelations() {
    const statement = this.db.prepare<unknown[], ArtistAlbumRelationRow>(
      "SELECT artist_id, album_id FROM artist_album ORDER BY artist_id ASC, album_id ASC",
    );

    for (const row of statement.iterate()) {
      yield {
        artistId: row.artist_id,
        albumId: row.album_id,
      };
    }
  }

  async listAlbumSongRelations() {
    return this.db
      .prepare<unknown[], AlbumSongRelationRow>(
        "SELECT album_id, song_id, track_number, disc_number, track_name FROM album_song ORDER BY album_id ASC, disc_number ASC, track_number ASC, song_id ASC",
      )
      .all()
      .map((row) => ({
        albumId: row.album_id,
        songId: row.song_id,
        trackNumber: row.track_number,
        discNumber: row.disc_number,
        trackName: row.track_name,
      }));
  }

  *iterateAlbumSongRelations() {
    const statement = this.db.prepare<unknown[], AlbumSongRelationRow>(
      "SELECT album_id, song_id, track_number, disc_number, track_name FROM album_song ORDER BY album_id ASC, disc_number ASC, track_number ASC, song_id ASC",
    );

    for (const row of statement.iterate()) {
      yield {
        albumId: row.album_id,
        songId: row.song_id,
        trackNumber: row.track_number,
        discNumber: row.disc_number,
        trackName: row.track_name,
      };
    }
  }

  async listArtistRelations() {
    return this.db
      .prepare<unknown[], ArtistRelationRow>(
        "SELECT artist_id, related_artist_id, relation_kind FROM artist_relation ORDER BY artist_id ASC, relation_kind ASC, related_artist_id ASC",
      )
      .all()
      .map((row) => ({
        artistId: row.artist_id,
        relatedArtistId: row.related_artist_id,
        relationKind: row.relation_kind,
      }));
  }

  *iterateArtistRelations() {
    const statement = this.db.prepare<unknown[], ArtistRelationRow>(
      "SELECT artist_id, related_artist_id, relation_kind FROM artist_relation ORDER BY artist_id ASC, relation_kind ASC, related_artist_id ASC",
    );

    for (const row of statement.iterate()) {
      yield {
        artistId: row.artist_id,
        relatedArtistId: row.related_artist_id,
        relationKind: row.relation_kind,
      };
    }
  }

  async persistEntity(entity: NormalizedEntity) {
    const table = entityTable(entity.entityType);
    const payloadJson = stableStringify(entity);
    const existing = this.db
      .prepare<[string], EntityRow>(`SELECT payload_json FROM ${table} WHERE entity_id = ?`)
      .get(entity.id);
    const previousEntity = existing ? (JSON.parse(existing.payload_json) as NormalizedEntity) : null;
    const isNew = !existing;
    const changed = existing?.payload_json !== payloadJson;

    if (!changed) {
      return { isNew, changed: false, previousEntity };
    }

    if (!this.dryRun) {
      this.persistEntityUnchecked(entity);
    }

    return { isNew, changed: true, previousEntity };
  }

  async loadFailedEntities() {
    const rows = this.db
      .prepare<unknown[], EntityRow>("SELECT payload_json FROM failed_entities ORDER BY failed_at DESC, entity_type ASC, vocadb_id ASC")
      .all();
    return rows.map((row) => JSON.parse(row.payload_json) as FailedEntityRecord);
  }

  async saveFailedEntities(entries: FailedEntityRecord[]) {
    if (this.dryRun) {
      return;
    }

    this.db.transaction((items: FailedEntityRecord[]) => {
      this.replaceFailedEntities(items);
    })(entries);
  }

  async saveLastRun(lastRun: LastRun) {
    if (this.dryRun) {
      return;
    }

    this.db.transaction((value: LastRun) => {
      this.insertSyncRun(value);
      this.saveSetting("last_run", value);
    })(lastRun);
  }

  async saveSeedsSnapshot(seeds: Seeds | Omit<Seeds, "modes">) {
    if (this.dryRun) {
      return;
    }

    this.saveSetting("seeds_snapshot", seeds);
  }

  async saveNewEntries(entries: EntitySummary[]) {
    if (this.dryRun) {
      return;
    }

    this.saveSetting("new_entries", entries);
  }

  saveDeriveRouteState(
    entityType: EntityType,
    items: Array<{ id: string; slug: string; displayName: string; pageNumber: number }>,
    totalItems: number,
    totalPages: number,
    pageSize: number,
  ) {
    if (this.dryRun) return;

    this.db.transaction(() => {
      this.db.prepare("DELETE FROM derive_route_state WHERE entity_type = ?").run(entityType);
      const insert = this.db.prepare(
        `INSERT INTO derive_route_state (entity_type, entity_id, slug, display_name, page_number)
         VALUES (?, ?, ?, ?, ?)`,
      );
      for (const item of items) {
        insert.run(entityType, item.id, item.slug, item.displayName, item.pageNumber);
      }
      this.db
        .prepare(
          `INSERT INTO derive_meta (entity_type, total_items, total_pages, page_size, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(entity_type) DO UPDATE SET
             total_items = excluded.total_items,
             total_pages = excluded.total_pages,
             page_size = excluded.page_size,
             updated_at = excluded.updated_at`,
        )
        .run(entityType, totalItems, totalPages, pageSize, new Date().toISOString());
    })();
  }

  loadDeriveRouteItem(entityType: EntityType, entityId: string) {
    return this.db
      .prepare<[string, string], { slug: string; display_name: string; page_number: number }>(
        `SELECT slug, display_name, page_number FROM derive_route_state WHERE entity_type = ? AND entity_id = ?`,
      )
      .get(entityType, entityId) ?? null;
  }

  loadDeriveMeta(entityType: EntityType) {
    return this.db
      .prepare<[string], { total_items: number; total_pages: number; page_size: number }>(
        `SELECT total_items, total_pages, page_size FROM derive_meta WHERE entity_type = ?`,
      )
      .get(entityType) ?? null;
  }

  listDerivePageNumbersForIds(entityType: EntityType, entityIds: Set<string>): number[] {
    if (entityIds.size === 0) return [];
    const ids = [...entityIds];
    const pageSet = new Set<number>();

    for (const chunk of chunked(ids, BULK_BATCH_SIZE)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const rows = this.db
        .prepare<unknown[], { page_number: number }>(
          `SELECT DISTINCT page_number FROM derive_route_state WHERE entity_type = ? AND entity_id IN (${placeholders})`,
        )
        .all(entityType, ...chunk);
      for (const row of rows) {
        pageSet.add(row.page_number);
      }
    }

    return [...pageSet].sort((a, b) => a - b);
  }

  close() {
    if (this.db.open) {
      this.db.pragma("wal_checkpoint(TRUNCATE)");
    }
    this.db.close();
  }

  private persistEntityUnchecked(entity: NormalizedEntity) {
    if (entity.entityType === "artist") {
      this.persistArtist(entity);
      return;
    }
    if (entity.entityType === "song") {
      this.persistSong(entity);
      return;
    }
    this.persistAlbum(entity);
  }

  private persistArtist(artist: Artist) {
    this.db.transaction((value: Artist) => {
      const payloadJson = stableStringify(value);
      this.db
        .prepare(
          `
            INSERT INTO artists (entity_id, vocadb_id, slug, display_name, synced_at, upstream_version, raw_hash, payload_json)
            VALUES (@entity_id, @vocadb_id, @slug, @display_name, @synced_at, @upstream_version, @raw_hash, @payload_json)
            ON CONFLICT(entity_id) DO UPDATE SET
              vocadb_id = excluded.vocadb_id,
              slug = excluded.slug,
              display_name = excluded.display_name,
              synced_at = excluded.synced_at,
              upstream_version = excluded.upstream_version,
              raw_hash = excluded.raw_hash,
              payload_json = excluded.payload_json
          `,
        )
        .run({
          entity_id: value.id,
          vocadb_id: value.vocadbId,
          slug: value.slug,
          display_name: value.displayName,
          synced_at: value.syncedAt,
          upstream_version: value.upstreamVersion ?? null,
          raw_hash: value.rawHash,
          payload_json: payloadJson,
        });

      this.db.prepare("DELETE FROM artist_relation WHERE artist_id = ?").run(value.id);

      const insertRelation = this.db.prepare<[string, string, string]>(
        "INSERT OR IGNORE INTO artist_relation (artist_id, related_artist_id, relation_kind) VALUES (?, ?, ?)",
      );

      for (const relatedArtistId of uniqueSorted(value.groups)) {
        insertRelation.run(value.id, relatedArtistId, "group");
      }
      for (const relatedArtistId of uniqueSorted(value.voicebanks)) {
        insertRelation.run(value.id, relatedArtistId, "voicebank");
      }
      for (const relatedArtistId of uniqueSorted(value.relatedArtistIds)) {
        insertRelation.run(value.id, relatedArtistId, "related");
      }
    })(artist);
  }

  private persistSong(song: Song) {
    this.db.transaction((value: Song) => {
      const payloadJson = stableStringify(value);
      this.db
        .prepare(
          `
            INSERT INTO songs (entity_id, vocadb_id, slug, display_name, synced_at, upstream_version, year, raw_hash, payload_json)
            VALUES (@entity_id, @vocadb_id, @slug, @display_name, @synced_at, @upstream_version, @year, @raw_hash, @payload_json)
            ON CONFLICT(entity_id) DO UPDATE SET
              vocadb_id = excluded.vocadb_id,
              slug = excluded.slug,
              display_name = excluded.display_name,
              synced_at = excluded.synced_at,
              upstream_version = excluded.upstream_version,
              year = excluded.year,
              raw_hash = excluded.raw_hash,
              payload_json = excluded.payload_json
          `,
        )
        .run({
          entity_id: value.id,
          vocadb_id: value.vocadbId,
          slug: value.slug,
          display_name: value.displayName,
          synced_at: value.syncedAt,
          upstream_version: value.upstreamVersion ?? null,
          year: entityYear(value),
          raw_hash: value.rawHash,
          payload_json: payloadJson,
        });

      this.db.prepare("DELETE FROM artist_song WHERE song_id = ?").run(value.id);

      const insertArtistSong = this.db.prepare<[string, string]>(
        "INSERT OR IGNORE INTO artist_song (artist_id, song_id) VALUES (?, ?)",
      );

      for (const artistId of uniqueSorted(value.artistIds)) {
        insertArtistSong.run(artistId, value.id);
      }
    })(song);
  }

  private persistAlbum(album: Album) {
    this.db.transaction((value: Album) => {
      const payloadJson = stableStringify(value);
      this.db
        .prepare(
          `
            INSERT INTO albums (entity_id, vocadb_id, slug, display_name, synced_at, upstream_version, year, raw_hash, payload_json)
            VALUES (@entity_id, @vocadb_id, @slug, @display_name, @synced_at, @upstream_version, @year, @raw_hash, @payload_json)
            ON CONFLICT(entity_id) DO UPDATE SET
              vocadb_id = excluded.vocadb_id,
              slug = excluded.slug,
              display_name = excluded.display_name,
              synced_at = excluded.synced_at,
              upstream_version = excluded.upstream_version,
              year = excluded.year,
              raw_hash = excluded.raw_hash,
              payload_json = excluded.payload_json
          `,
        )
        .run({
          entity_id: value.id,
          vocadb_id: value.vocadbId,
          slug: value.slug,
          display_name: value.displayName,
          synced_at: value.syncedAt,
          upstream_version: value.upstreamVersion ?? null,
          year: entityYear(value),
          raw_hash: value.rawHash,
          payload_json: payloadJson,
        });

      this.db.prepare("DELETE FROM artist_album WHERE album_id = ?").run(value.id);
      this.db.prepare("DELETE FROM album_song WHERE album_id = ?").run(value.id);

      const insertArtistAlbum = this.db.prepare<[string, string]>(
        "INSERT OR IGNORE INTO artist_album (artist_id, album_id) VALUES (?, ?)",
      );
      const insertAlbumSong = this.db.prepare<[string, string, number, number, string]>(
        "INSERT OR IGNORE INTO album_song (album_id, song_id, track_number, disc_number, track_name) VALUES (?, ?, ?, ?, ?)",
      );

      for (const artistId of uniqueSorted(value.artistIds)) {
        insertArtistAlbum.run(artistId, value.id);
      }

      const trackEntries =
        value.tracks.length > 0
          ? value.tracks.map((track) => ({
              songId: track.songId,
              trackNumber: track.trackNumber ?? 0,
              discNumber: track.discNumber ?? 0,
              trackName: track.name,
            }))
          : uniqueSorted(value.songIds).map((songId) => ({
              songId,
              trackNumber: 0,
              discNumber: 0,
              trackName: songId,
            }));

      for (const track of trackEntries) {
        insertAlbumSong.run(value.id, track.songId, track.trackNumber, track.discNumber, track.trackName);
      }
    })(album);
  }

  private replaceFailedEntities(entries: FailedEntityRecord[]) {
    this.db.prepare("DELETE FROM failed_entities").run();

    const insertFailedEntity = this.db.prepare(
      `
        INSERT INTO failed_entities (
          entity_type,
          vocadb_id,
          depth,
          discovered_from,
          reason,
          known_version,
          retry_count,
          error,
          failed_at,
          payload_json
        )
        VALUES (
          @entity_type,
          @vocadb_id,
          @depth,
          @discovered_from,
          @reason,
          @known_version,
          @retry_count,
          @error,
          @failed_at,
          @payload_json
        )
      `,
    );

    for (const entry of entries) {
      insertFailedEntity.run({
        entity_type: entry.entityType,
        vocadb_id: entry.vocadbId,
        depth: entry.depth,
        discovered_from: entry.discoveredFrom,
        reason: entry.reason,
        known_version: entry.knownVersion ?? null,
        retry_count: entry.retryCount ?? 0,
        error: entry.error,
        failed_at: entry.failedAt,
        payload_json: stableStringify(entry),
      });
    }
  }

  private insertSyncRun(lastRun: LastRun) {
    this.db
      .prepare(
        `
          INSERT INTO sync_runs (mode, dry_run, started_at, finished_at, stats_json)
          VALUES (@mode, @dry_run, @started_at, @finished_at, @stats_json)
        `,
      )
      .run({
        mode: lastRun.mode,
        dry_run: lastRun.dryRun ? 1 : 0,
        started_at: lastRun.startedAt,
        finished_at: lastRun.finishedAt,
        stats_json: stableStringify(lastRun),
      });
  }

  private loadSetting<T>(key: string): T | null {
    const row = this.db.prepare<[string], EntityRow>("SELECT value_json AS payload_json FROM settings_snapshot WHERE key = ?").get(key);
    if (!row) {
      return null;
    }

    return JSON.parse(row.payload_json) as T;
  }

  private saveSetting(
    key: string,
    value: Seeds | Omit<Seeds, "modes"> | LastRun | EntitySummary[] | ReconcileState | CatalogScanState,
  ) {
    this.db
      .prepare(
        `
          INSERT INTO settings_snapshot (key, value_json, updated_at)
          VALUES (@key, @value_json, @updated_at)
          ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = excluded.updated_at
        `,
      )
      .run({
        key,
        value_json: stableStringify(value),
        updated_at: new Date().toISOString(),
      });
  }

  private deleteSetting(key: string) {
    this.db.prepare("DELETE FROM settings_snapshot WHERE key = ?").run(key);
  }

  private bulkArtistCounts(ids: string[]): Map<string, { songCount: number; albumCount: number }> {
    const result = new Map<string, { songCount: number; albumCount: number }>();
    for (const id of ids) {
      result.set(id, { songCount: 0, albumCount: 0 });
    }
    if (ids.length === 0) return result;

    for (const chunk of chunked(ids, BULK_BATCH_SIZE)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const songRows = this.db
        .prepare<unknown[], { artist_id: string; cnt: number }>(
          `SELECT artist_id, COUNT(*) AS cnt FROM artist_song WHERE artist_id IN (${placeholders}) GROUP BY artist_id`,
        )
        .all(...chunk);
      const albumRows = this.db
        .prepare<unknown[], { artist_id: string; cnt: number }>(
          `SELECT artist_id, COUNT(*) AS cnt FROM artist_album WHERE artist_id IN (${placeholders}) GROUP BY artist_id`,
        )
        .all(...chunk);
      for (const row of songRows) {
        const entry = result.get(row.artist_id);
        if (entry) entry.songCount = row.cnt;
      }
      for (const row of albumRows) {
        const entry = result.get(row.artist_id);
        if (entry) entry.albumCount = row.cnt;
      }
    }
    return result;
  }

  private bulkSongCounts(ids: string[]): Map<string, { artistCount: number; albumCount: number }> {
    const result = new Map<string, { artistCount: number; albumCount: number }>();
    for (const id of ids) {
      result.set(id, { artistCount: 0, albumCount: 0 });
    }
    if (ids.length === 0) return result;

    for (const chunk of chunked(ids, BULK_BATCH_SIZE)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const artistRows = this.db
        .prepare<unknown[], { song_id: string; cnt: number }>(
          `SELECT song_id, COUNT(*) AS cnt FROM artist_song WHERE song_id IN (${placeholders}) GROUP BY song_id`,
        )
        .all(...chunk);
      const albumRows = this.db
        .prepare<unknown[], { song_id: string; cnt: number }>(
          `SELECT song_id, COUNT(*) AS cnt FROM album_song WHERE song_id IN (${placeholders}) GROUP BY song_id`,
        )
        .all(...chunk);
      for (const row of artistRows) {
        const entry = result.get(row.song_id);
        if (entry) entry.artistCount = row.cnt;
      }
      for (const row of albumRows) {
        const entry = result.get(row.song_id);
        if (entry) entry.albumCount = row.cnt;
      }
    }
    return result;
  }

  private bulkAlbumCounts(ids: string[]): Map<string, { trackCount: number }> {
    const result = new Map<string, { trackCount: number }>();
    for (const id of ids) {
      result.set(id, { trackCount: 0 });
    }
    if (ids.length === 0) return result;

    for (const chunk of chunked(ids, BULK_BATCH_SIZE)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const trackRows = this.db
        .prepare<unknown[], { album_id: string; cnt: number }>(
          `SELECT album_id, COUNT(*) AS cnt FROM album_song WHERE album_id IN (${placeholders}) GROUP BY album_id`,
        )
        .all(...chunk);
      for (const row of trackRows) {
        const entry = result.get(row.album_id);
        if (entry) entry.trackCount = row.cnt;
      }
    }
    return result;
  }

  private async listSummaryPageForEntityId(entityType: EntityType, entityId: string) {
    if (entityType === "artist") {
      return this.listArtistSummariesForQuery("SELECT payload_json FROM artists WHERE entity_id = ?", entityId);
    }
    if (entityType === "song") {
      return this.listSongSummariesForQuery("SELECT payload_json FROM songs WHERE entity_id = ?", entityId);
    }
    return this.listAlbumSummariesForQuery("SELECT payload_json FROM albums WHERE entity_id = ?", entityId);
  }

  private listArtistSummariesForQuery(query: string, ...params: unknown[]): ArtistSummary[] {
    const rows = this.db.prepare(query).all(...params) as EntityRow[];
    if (rows.length === 0) {
      return [];
    }

    const ids = rows.map((row) => parseEntityPayload<Artist>(row.payload_json).id);
    const counts = this.bulkArtistCounts(ids);

    return rows.map((row) => {
      const artist = parseEntityPayload<Artist>(row.payload_json);
      const c = counts.get(artist.id);
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
        songCount: c?.songCount ?? 0,
        albumCount: c?.albumCount ?? 0,
      };
    });
  }

  private listSongSummariesForQuery(query: string, ...params: unknown[]): SongSummary[] {
    const rows = this.db.prepare(query).all(...params) as EntityRow[];
    if (rows.length === 0) {
      return [];
    }

    const ids = rows.map((row) => parseEntityPayload<Song>(row.payload_json).id);
    const counts = this.bulkSongCounts(ids);

    return rows.map((row) => {
      const song = parseEntityPayload<Song>(row.payload_json);
      const c = counts.get(song.id);
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
        artistCount: c?.artistCount ?? 0,
        albumCount: c?.albumCount ?? 0,
        tags: song.tags,
      };
    });
  }

  private listAlbumSummariesForQuery(query: string, ...params: unknown[]): AlbumSummary[] {
    const rows = this.db.prepare(query).all(...params) as EntityRow[];
    if (rows.length === 0) {
      return [];
    }

    const ids = rows.map((row) => parseEntityPayload<Album>(row.payload_json).id);
    const counts = this.bulkAlbumCounts(ids);

    return rows.map((row) => {
      const album = parseEntityPayload<Album>(row.payload_json);
      const c = counts.get(album.id);
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
        trackCount: c?.trackCount || album.tracks.length,
      };
    });
  }
}
