import { getBrowserSqliteWorker, loadDbSnapshotManifest } from "./browser-sqlite";
import type { EntityType } from "./models";

export type SearchResult = {
  entityType: EntityType;
  slug: string;
  displayName: string;
};

function prefixUpperBound(term: string): string {
  if (term.length === 0) return "\uffff";
  return term.slice(0, -1) + String.fromCharCode(term.charCodeAt(term.length - 1) + 1);
}

export async function isSearchAvailable(): Promise<boolean> {
  const manifest = await loadDbSnapshotManifest();
  return manifest.available && !!manifest.configUrl;
}

export async function searchEntities(
  query: string,
  limit = 40,
): Promise<SearchResult[]> {
  const term = query.toLowerCase().trim().replace(/[^\p{L}\p{N}]/gu, "");
  if (term.length < 2) return [];

  const worker = await getBrowserSqliteWorker();
  if (!worker) return [];

  const upper = prefixUpperBound(term);

  const rows = (await worker.db.query(
    `SELECT DISTINCT entity_type, slug, display_name
     FROM search_index
     WHERE term >= ? AND term < ?
     ORDER BY display_name
     LIMIT ?`,
    [term, upper, limit],
  )) as Array<{ entity_type: string; slug: string; display_name: string }>;

  return rows.map((row) => ({
    entityType: row.entity_type as EntityType,
    slug: row.slug,
    displayName: row.display_name,
  }));
}
