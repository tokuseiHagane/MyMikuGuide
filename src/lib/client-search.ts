import type { EntityType } from "./models";

export type SearchResult = {
  entityType: EntityType;
  slug: string;
  displayName: string;
  preview: string;
};

const TYPE_MAP: Record<string, EntityType> = { a: "artist", s: "song", l: "album" };
const SHARD_COUNT = 256;
const WORD_RE = /[\p{L}\p{N}]+/gu;

const BASE_URL = import.meta.env.BASE_URL;

function withBase(p: string) {
  const base = BASE_URL.endsWith("/") ? BASE_URL.slice(0, -1) : BASE_URL;
  return p.startsWith("/") ? `${base}${p}` : p;
}

function shardId(prefix: string): number {
  let h = 0;
  for (let i = 0; i < prefix.length; i++) {
    h = ((h << 5) - h + prefix.charCodeAt(i)) | 0;
  }
  return ((h % SHARD_COUNT) + SHARD_COUNT) % SHARD_COUNT;
}

type ShardEntry = [string, string, string, string?];
type ShardData = Record<string, ShardEntry[]>;

const shardCache = new Map<number, ShardData>();

async function loadShard(sid: number): Promise<ShardData> {
  const cached = shardCache.get(sid);
  if (cached) return cached;

  try {
    const res = await fetch(withBase(`/search-index/${sid}.json`));
    if (!res.ok) {
      shardCache.set(sid, {});
      return {};
    }
    const data = (await res.json()) as ShardData;
    shardCache.set(sid, data);
    return data;
  } catch {
    shardCache.set(sid, {});
    return {};
  }
}

function tokenize(text: string): string[] {
  const words = text.toLowerCase().match(WORD_RE) ?? [];
  return [...new Set(words)].filter((w) => w.length >= 2);
}

function findMatchingUids(shard: ShardData, term: string): Map<string, ShardEntry> {
  const matches = new Map<string, ShardEntry>();
  for (const [key, entries] of Object.entries(shard)) {
    if (!key.startsWith(term)) continue;
    for (const entry of entries) {
      const uid = `${entry[0]}:${entry[1]}`;
      if (!matches.has(uid)) {
        matches.set(uid, entry);
      }
    }
  }
  return matches;
}

export async function searchEntities(
  query: string,
  limit = 50,
  onProgress?: (stage: string) => void,
): Promise<SearchResult[]> {
  const terms = tokenize(query);
  if (terms.length === 0) return [];

  const shardIds = [...new Set(terms.map((t) => shardId(t.slice(0, 2))))];

  onProgress?.("Загрузка индекса…");
  const shards = await Promise.all(shardIds.map(loadShard));
  const shardBySid = new Map(shardIds.map((sid, i) => [sid, shards[i]]));

  onProgress?.("Поиск…");

  let intersection: Map<string, ShardEntry> | null = null;

  for (const term of terms) {
    const sid = shardId(term.slice(0, 2));
    const shard = shardBySid.get(sid) ?? {};
    const matches = findMatchingUids(shard, term);

    if (intersection === null) {
      intersection = matches;
    } else {
      for (const uid of intersection.keys()) {
        if (!matches.has(uid)) {
          intersection.delete(uid);
        }
      }
    }

    if (intersection.size === 0) return [];
  }

  if (!intersection) return [];

  const results: SearchResult[] = [];
  for (const [, [code, slug, displayName, preview]] of intersection) {
    results.push({
      entityType: TYPE_MAP[code] ?? "artist",
      slug,
      displayName,
      preview: preview ?? "",
    });
    if (results.length >= limit) break;
  }

  return results;
}
