import type { EntityType } from "./models";

export type SearchResult = {
  entityType: EntityType;
  slug: string;
  displayName: string;
};

const TYPE_MAP: Record<string, EntityType> = { a: "artist", s: "song", l: "album" };
const SHARD_COUNT = 256;

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

type ShardData = Record<string, [string, string, string][]>;

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

export async function searchEntities(
  query: string,
  limit = 50,
  onProgress?: (stage: string) => void,
): Promise<SearchResult[]> {
  const term = query.toLowerCase().trim().replace(/[^\p{L}\p{N}]/gu, "");
  if (term.length < 2) return [];

  const prefix = term.slice(0, 2);
  const sid = shardId(prefix);

  onProgress?.("Загрузка индекса…");
  const shard = await loadShard(sid);

  onProgress?.("Поиск…");

  const seen = new Set<string>();
  const results: SearchResult[] = [];

  for (const [key, entries] of Object.entries(shard)) {
    if (!key.startsWith(term)) continue;
    for (const [code, slug, displayName] of entries) {
      const uid = `${code}:${slug}`;
      if (seen.has(uid)) continue;
      seen.add(uid);
      results.push({
        entityType: TYPE_MAP[code] ?? "artist",
        slug,
        displayName,
      });
      if (results.length >= limit) return results;
    }
  }

  return results;
}
