import type { EntityType } from "./models";

function entitySegment(entityType: EntityType) {
  if (entityType === "artist") {
    return "artists";
  }
  if (entityType === "song") {
    return "songs";
  }
  return "albums";
}

export function detailShellPath(entityType: EntityType, slug: string) {
  return `/${entitySegment(entityType)}/view/?slug=${encodeURIComponent(slug)}`;
}

export function detailDataPath(entityType: EntityType, slug: string) {
  return `/derived/detail/${entityType}/${encodeURIComponent(slug)}.json`;
}
