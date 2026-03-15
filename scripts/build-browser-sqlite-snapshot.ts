import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, open, rename, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";

const repoRoot = process.cwd();
const defaultSourcePath = path.join(repoRoot, "data", "db", "vocadb.sqlite");
const defaultOutputRoot = path.join(repoRoot, "dist", "sqlite");
const defaultManifestPath = path.join(repoRoot, "dist", "meta", "db-snapshot.json");
const defaultSnapshotFileName = "vocadb.sqlite";

type DbSnapshotManifest = {
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

function envValue(name: string, fallback: string) {
  const value = process.env[name]?.trim();
  return value && value.length > 0 ? value : fallback;
}

function envInteger(name: string, fallback: number) {
  const raw = process.env[name]?.trim();
  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Environment variable ${name} must be a positive integer`);
  }

  return parsed;
}

function buildUnavailableManifest(): DbSnapshotManifest {
  return {
    available: false,
    provider: "pages-sqlite",
    version: null,
    configUrl: null,
    databaseUrl: null,
    sha256: null,
    bytes: null,
    pageSize: null,
    updatedAt: null,
  };
}

async function sha256File(targetPath: string) {
  const hash = createHash("sha256");
  const stream = createReadStream(targetPath);

  await new Promise<void>((resolve, reject) => {
    stream.on("data", (chunk) => {
      hash.update(chunk);
    });
    stream.on("end", () => {
      resolve();
    });
    stream.on("error", (error) => {
      reject(error);
    });
  });

  return hash.digest("hex");
}

async function writeManifest(manifestPath: string, manifest: DbSnapshotManifest) {
  await mkdir(path.dirname(manifestPath), { recursive: true });
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
}

async function main() {
  const sourcePath = envValue("PAGES_DB_SOURCE_PATH", defaultSourcePath);
  const outputRoot = envValue("PAGES_DB_OUTPUT_ROOT", defaultOutputRoot);
  const manifestPath = envValue("PAGES_DB_MANIFEST_PATH", defaultManifestPath);
  const version = envValue("PAGES_DB_VERSION", `local-${Date.now()}`);
  const updatedAt = envValue("PAGES_DB_UPDATED_AT", new Date().toISOString());
  const pageSize = envInteger("PAGES_DB_PAGE_SIZE", 4096);
  const snapshotFileName = envValue("PAGES_DB_FILE_NAME", defaultSnapshotFileName);

  try {
    await stat(sourcePath);
  } catch {
    await writeManifest(manifestPath, buildUnavailableManifest());
    console.log(`Skipped browser SQLite snapshot because ${sourcePath} does not exist.`);
    return;
  }

  const versionDir = path.join(outputRoot, version);
  const targetSqlitePath = path.join(versionDir, snapshotFileName);
  const targetConfigPath = path.join(versionDir, "config.json");
  const backupPath = path.join(versionDir, `${snapshotFileName}.backup`);

  await mkdir(versionDir, { recursive: true });
  await rm(backupPath, { force: true });

  const sourceDb = new Database(sourcePath, { readonly: true, fileMustExist: true });
  try {
    await sourceDb.backup(backupPath);
  } finally {
    sourceDb.close();
  }

  await rm(targetSqlitePath, { force: true });

  const db = new Database(backupPath);
  try {
    const serverOnlyTables = [
      "catalog_manifest",
      "derive_route_state",
      "derive_meta",
      "full_sync_state",
      "sync_runs",
      "settings_snapshot",
      "failed_entities",
    ];

    for (const table of serverOnlyTables) {
      db.exec(`DROP TABLE IF EXISTS ${table}`);
    }

    const serverOnlyIndexes = db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'
         AND (name LIKE '%synced_at%' OR name LIKE '%display_name%' OR name LIKE '%year%'
              OR name LIKE '%catalog%' OR name LIKE '%derive%' OR name LIKE '%run_token%'
              OR name LIKE '%entity_hydrate%')`,
      )
      .all() as Array<{ name: string }>;

    for (const idx of serverOnlyIndexes) {
      db.exec(`DROP INDEX IF EXISTS "${idx.name}"`);
    }

    console.log(
      `Stripped ${serverOnlyTables.length} server-only tables and ${serverOnlyIndexes.length} server-only indexes from browser snapshot.`,
    );

    db.pragma("journal_mode = DELETE");
    db.pragma(`page_size = ${pageSize}`);
    db.exec("VACUUM");
    db.exec("PRAGMA optimize");
    db.pragma("wal_checkpoint(TRUNCATE)");
  } finally {
    db.close();
  }

  const optimizedDb = new Database(backupPath, { readonly: true });
  const effectivePageSize = Number(optimizedDb.pragma("page_size", { simple: true }));
  optimizedDb.close();

  await rm(targetSqlitePath, { force: true });
  await rename(backupPath, targetSqlitePath);

  const targetStat = await stat(targetSqlitePath);
  const sha256 = await sha256File(targetSqlitePath);
  const cacheBust = sha256.slice(0, 16);
  const snapshotRoot = `/sqlite/${version}`;

  const SERVER_CHUNK_SIZE = 4 * 1024 * 1024;
  const totalChunks = Math.ceil(targetStat.size / SERVER_CHUNK_SIZE);
  const suffixLength = String(totalChunks - 1).length;

  console.log(
    `Splitting ${targetStat.size} bytes into ${totalChunks} chunks (${SERVER_CHUNK_SIZE} bytes each, suffix length ${suffixLength})…`,
  );

  const fd = await open(targetSqlitePath, "r");
  for (let i = 0; i < totalChunks; i++) {
    const offset = i * SERVER_CHUNK_SIZE;
    const length = Math.min(SERVER_CHUNK_SIZE, targetStat.size - offset);
    const buf = Buffer.alloc(length);
    await fd.read(buf, 0, length, offset);
    const chunkPath = path.join(versionDir, `${snapshotFileName}.${String(i).padStart(suffixLength, "0")}`);
    await writeFile(chunkPath, buf);
  }
  await fd.close();
  await rm(targetSqlitePath);

  const config = {
    serverMode: "chunked" as const,
    requestChunkSize: effectivePageSize,
    serverChunkSize: SERVER_CHUNK_SIZE,
    urlPrefix: `${snapshotFileName}.`,
    databaseLengthBytes: targetStat.size,
    suffixLength,
    cacheBust,
  };

  await writeFile(targetConfigPath, `${JSON.stringify(config, null, 2)}\n`, "utf8");

  const manifest: DbSnapshotManifest = {
    available: true,
    provider: "pages-sqlite",
    version,
    configUrl: `${snapshotRoot}/config.json`,
    databaseUrl: `${snapshotRoot}/${snapshotFileName}`,
    sha256,
    bytes: targetStat.size,
    pageSize: effectivePageSize,
    updatedAt,
  };

  await writeManifest(manifestPath, manifest);

  console.log(
    `Built browser SQLite snapshot ${version} (${targetStat.size} bytes, ${totalChunks} chunks, page_size=${effectivePageSize}, sha256=${sha256})`,
  );
}

await main();
