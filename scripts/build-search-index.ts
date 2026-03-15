import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { toRomaji } from "wanakana";

const repoRoot = process.cwd();
const dbPath = path.join(repoRoot, "data", "db", "vocadb.sqlite");
const outputDir = path.join(repoRoot, "dist", "search-index");

const SHARD_COUNT = 256;

const KANA_RE = /[\u3040-\u309f\u30a0-\u30ff]/;
const WORD_RE = /[\p{L}\p{N}]+/gu;

function shardId(prefix: string): number {
  let h = 0;
  for (let i = 0; i < prefix.length; i++) {
    h = ((h << 5) - h + prefix.charCodeAt(i)) | 0;
  }
  return ((h % SHARD_COUNT) + SHARD_COUNT) % SHARD_COUNT;
}

function tokenize(text: string): string[] {
  const words = text.toLowerCase().match(WORD_RE) ?? [];
  return [...new Set(words)].filter((w) => w.length >= 2);
}

function romajiTokens(text: string): string[] {
  if (!KANA_RE.test(text)) return [];
  try {
    const rom = toRomaji(text).toLowerCase();
    return tokenize(rom);
  } catch {
    return [];
  }
}

type ShardEntry = [string, string, string]; // [entityType code, slug, displayName]

async function main() {
  const started = Date.now();
  const db = new Database(dbPath, { readonly: true });

  const tables: Array<{ table: string; code: string }> = [
    { table: "artists", code: "a" },
    { table: "songs", code: "s" },
    { table: "albums", code: "l" },
  ];

  const shards: Map<number, Map<string, ShardEntry[]>> = new Map();
  for (let i = 0; i < SHARD_COUNT; i++) shards.set(i, new Map());

  let totalEntities = 0;
  let totalTermPairs = 0;

  for (const { table, code } of tables) {
    const rows = db
      .prepare<
        unknown[],
        { slug: string; display_name: string; payload_json: string }
      >(`SELECT slug, display_name, payload_json FROM ${table}`)
      .all();

    for (const row of rows) {
      let additionalNames: string[] = [];
      try {
        const payload = JSON.parse(row.payload_json);
        additionalNames = Array.isArray(payload.additionalNames)
          ? payload.additionalNames.filter((n: unknown) => typeof n === "string")
          : [];
      } catch {
        // skip
      }

      const allNames = [row.display_name, ...additionalNames];
      const terms = new Set<string>();
      for (const name of allNames) {
        for (const t of tokenize(name)) terms.add(t);
        for (const t of romajiTokens(name)) terms.add(t);
      }

      const entry: ShardEntry = [code, row.slug, row.display_name];

      for (const term of terms) {
        const prefix = term.slice(0, 2);
        const sid = shardId(prefix);
        const shard = shards.get(sid)!;
        let bucket = shard.get(term);
        if (!bucket) {
          bucket = [];
          shard.set(term, bucket);
        }
        bucket.push(entry);
        totalTermPairs++;
      }

      totalEntities++;
    }

    console.log(`  ${table}: ${rows.length} entities processed`);
  }

  db.close();

  await rm(outputDir, { recursive: true, force: true });
  await mkdir(outputDir, { recursive: true });

  let maxShardBytes = 0;

  for (const [sid, termMap] of shards) {
    if (termMap.size === 0) continue;
    const obj: Record<string, ShardEntry[]> = {};
    for (const [term, entries] of termMap) {
      obj[term] = entries;
    }
    const json = JSON.stringify(obj);
    await writeFile(path.join(outputDir, `${sid}.json`), json);
    if (json.length > maxShardBytes) maxShardBytes = json.length;
  }

  const manifest = {
    version: 1,
    shardCount: SHARD_COUNT,
    totalEntities,
  };
  await writeFile(
    path.join(outputDir, "manifest.json"),
    JSON.stringify(manifest),
  );

  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  console.log(
    `Search index: ${totalEntities} entities, ${totalTermPairs} term-entity pairs, ${SHARD_COUNT} shards (max ${(maxShardBytes / 1024 / 1024).toFixed(1)}MB) in ${elapsed}s`,
  );
}

await main();
