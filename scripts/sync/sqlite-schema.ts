import type Database from "better-sqlite3";

export function initializeSqliteSchema(db: Database.Database) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS artists (
      entity_id TEXT PRIMARY KEY,
      vocadb_id INTEGER NOT NULL UNIQUE,
      slug TEXT NOT NULL UNIQUE,
      display_name TEXT NOT NULL,
      synced_at TEXT NOT NULL,
      upstream_version INTEGER,
      raw_hash TEXT NOT NULL,
      payload_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS songs (
      entity_id TEXT PRIMARY KEY,
      vocadb_id INTEGER NOT NULL UNIQUE,
      slug TEXT NOT NULL UNIQUE,
      display_name TEXT NOT NULL,
      synced_at TEXT NOT NULL,
      upstream_version INTEGER,
      year INTEGER,
      raw_hash TEXT NOT NULL,
      payload_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS albums (
      entity_id TEXT PRIMARY KEY,
      vocadb_id INTEGER NOT NULL UNIQUE,
      slug TEXT NOT NULL UNIQUE,
      display_name TEXT NOT NULL,
      synced_at TEXT NOT NULL,
      upstream_version INTEGER,
      year INTEGER,
      raw_hash TEXT NOT NULL,
      payload_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS artist_song (
      artist_id TEXT NOT NULL,
      song_id TEXT NOT NULL,
      PRIMARY KEY (artist_id, song_id)
    );

    CREATE TABLE IF NOT EXISTS artist_album (
      artist_id TEXT NOT NULL,
      album_id TEXT NOT NULL,
      PRIMARY KEY (artist_id, album_id)
    );

    CREATE TABLE IF NOT EXISTS album_song (
      album_id TEXT NOT NULL,
      song_id TEXT NOT NULL,
      track_number INTEGER NOT NULL DEFAULT 0,
      disc_number INTEGER NOT NULL DEFAULT 0,
      track_name TEXT NOT NULL,
      PRIMARY KEY (album_id, song_id, track_number, disc_number)
    );

    CREATE TABLE IF NOT EXISTS artist_relation (
      artist_id TEXT NOT NULL,
      related_artist_id TEXT NOT NULL,
      relation_kind TEXT NOT NULL,
      PRIMARY KEY (artist_id, related_artist_id, relation_kind)
    );

    CREATE TABLE IF NOT EXISTS failed_entities (
      entity_type TEXT NOT NULL,
      vocadb_id INTEGER NOT NULL,
      depth INTEGER NOT NULL,
      discovered_from TEXT NOT NULL,
      reason TEXT NOT NULL,
      known_version INTEGER,
      retry_count INTEGER NOT NULL DEFAULT 0,
      error TEXT NOT NULL,
      failed_at TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      PRIMARY KEY (entity_type, vocadb_id, depth, discovered_from, reason)
    );

    CREATE TABLE IF NOT EXISTS catalog_manifest (
      entity_type TEXT NOT NULL,
      vocadb_id INTEGER NOT NULL,
      display_name TEXT NOT NULL,
      upstream_version INTEGER,
      published_at TEXT,
      created_at TEXT,
      release_date TEXT,
      last_seen_run_token TEXT NOT NULL,
      needs_hydrate INTEGER NOT NULL DEFAULT 0,
      hydrate_state TEXT NOT NULL DEFAULT 'pending',
      hydrated_at TEXT,
      last_error TEXT,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (entity_type, vocadb_id)
    );

    CREATE TABLE IF NOT EXISTS full_sync_state (
      state_key TEXT PRIMARY KEY,
      phase TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      next_start INTEGER NOT NULL,
      page_size INTEGER NOT NULL,
      pass INTEGER NOT NULL,
      run_token TEXT NOT NULL,
      pages_fetched INTEGER NOT NULL DEFAULT 0,
      unique_ids_seen INTEGER NOT NULL DEFAULT 0,
      duplicate_ids_seen INTEGER NOT NULL DEFAULT 0,
      hydrated_count INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sync_runs (
      run_id INTEGER PRIMARY KEY AUTOINCREMENT,
      mode TEXT NOT NULL,
      dry_run INTEGER NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      stats_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS settings_snapshot (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS derive_route_state (
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      slug TEXT NOT NULL,
      display_name TEXT NOT NULL,
      page_number INTEGER NOT NULL,
      PRIMARY KEY (entity_type, entity_id)
    );

    CREATE TABLE IF NOT EXISTS derive_meta (
      entity_type TEXT PRIMARY KEY,
      total_items INTEGER NOT NULL,
      total_pages INTEGER NOT NULL,
      page_size INTEGER NOT NULL,
      updated_at TEXT NOT NULL
    );

    -- UNIQUE slug already provides implicit index; these composites cover ORDER BY patterns
    CREATE INDEX IF NOT EXISTS idx_artists_display_name_slug ON artists(display_name, slug, entity_id);
    CREATE INDEX IF NOT EXISTS idx_songs_display_name_slug ON songs(display_name, slug, entity_id);
    CREATE INDEX IF NOT EXISTS idx_albums_display_name_slug ON albums(display_name, slug, entity_id);

    CREATE INDEX IF NOT EXISTS idx_artists_synced_at ON artists(synced_at DESC, entity_id);
    CREATE INDEX IF NOT EXISTS idx_songs_synced_at ON songs(synced_at DESC, entity_id);
    CREATE INDEX IF NOT EXISTS idx_albums_synced_at ON albums(synced_at DESC, entity_id);

    CREATE INDEX IF NOT EXISTS idx_songs_year ON songs(year);
    CREATE INDEX IF NOT EXISTS idx_albums_year ON albums(year);

    CREATE INDEX IF NOT EXISTS idx_artist_song_song_id ON artist_song(song_id);
    CREATE INDEX IF NOT EXISTS idx_artist_album_album_id ON artist_album(album_id);
    CREATE INDEX IF NOT EXISTS idx_album_song_song_id ON album_song(song_id);
    CREATE INDEX IF NOT EXISTS idx_album_song_order ON album_song(album_id, disc_number, track_number, song_id);
    CREATE INDEX IF NOT EXISTS idx_artist_relation_related ON artist_relation(artist_id, relation_kind, related_artist_id);

    CREATE INDEX IF NOT EXISTS idx_failed_entities_failed_at ON failed_entities(failed_at);
    CREATE INDEX IF NOT EXISTS idx_catalog_manifest_entity_hydrate ON catalog_manifest(entity_type, needs_hydrate, vocadb_id);
    CREATE INDEX IF NOT EXISTS idx_catalog_manifest_run_token ON catalog_manifest(last_seen_run_token);
    CREATE INDEX IF NOT EXISTS idx_catalog_manifest_created ON catalog_manifest(entity_type, created_at DESC, vocadb_id);
  `);
}
