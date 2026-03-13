import type { Album, Artist, EntityType, Song } from "./models";

type LinkedEntity = Artist | Song | Album;

const BASE_URL = import.meta.env.BASE_URL;

export function withBase(path: string) {
  if (!path.startsWith("/")) {
    return path;
  }

  const normalizedBase = BASE_URL.endsWith("/") ? BASE_URL.slice(0, -1) : BASE_URL;
  return `${normalizedBase}${path}`;
}

export function formatDateTime(value?: string | null) {
  if (!value) {
    return "Неизвестно";
  }

  return new Date(value).toLocaleString("ru-RU");
}

export function entityTypeLabel(entityType: EntityType) {
  if (entityType === "artist") {
    return "Артист";
  }
  if (entityType === "song") {
    return "Песня";
  }
  return "Альбом";
}

export function entityBadge(entity: LinkedEntity) {
  if (entity.entityType === "artist") {
    return entity.artistType;
  }
  if (entity.entityType === "song") {
    return entity.songType;
  }
  return entity.albumType;
}

export function entityImage(entity: LinkedEntity) {
  return entity.primaryImage ?? null;
}

export function entityDescription(entity: LinkedEntity) {
  if (entity.entityType === "artist") {
    return entity.descriptionShort;
  }

  if (entity.entityType === "song") {
    return entity.tags.slice(0, 4).join(" • ");
  }

  const trackCount = entity.tracks.length;
  return trackCount > 0 ? `${trackCount} треков` : "";
}

export function entityMeta(entity: LinkedEntity) {
  if (entity.entityType === "artist") {
    return [
      `${entity.songIds.length} песен`,
      `${entity.albumIds.length} альбомов`,
      `sync ${formatDateTime(entity.syncedAt)}`,
    ];
  }

  if (entity.entityType === "song") {
    return [
      entity.year ? String(entity.year) : "Без даты",
      `${entity.artistIds.length} артистов`,
      entity.durationSeconds ? `${entity.durationSeconds} сек.` : "Длительность неизвестна",
    ];
  }

  return [
    entity.year ? String(entity.year) : "Без даты",
    `${entity.songIds.length} треков`,
    entity.catalogNumber ? `Cat. ${entity.catalogNumber}` : "Без каталожного номера",
  ];
}

export function buildEntityMap<T extends LinkedEntity>(items: T[]) {
  return new Map(items.map((item) => [item.id, item]));
}

export function linkedEntities<T extends LinkedEntity>(ids: string[], source: Map<string, T>) {
  return ids.map((id) => source.get(id)).filter((item): item is T => Boolean(item));
}
