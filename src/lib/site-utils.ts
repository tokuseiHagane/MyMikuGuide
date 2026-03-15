import type { EntitySummary, EntityType } from "./models";
import { detailShellPath } from "./entity-paths";

type LinkedEntity = EntitySummary;

const BASE_URL = import.meta.env.BASE_URL;

export function withBase(path: string) {
  if (!path.startsWith("/")) {
    return path;
  }

  const normalizedBase = BASE_URL.endsWith("/") ? BASE_URL.slice(0, -1) : BASE_URL;
  return `${normalizedBase}${path}`;
}

export function entityHref(entity: { entityType: EntityType; slug: string }) {
  return withBase(detailShellPath(entity.entityType, entity.slug));
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

  const trackCount = entity.trackCount;
  return trackCount > 0 ? `${trackCount} треков` : "";
}

export function entityMeta(entity: LinkedEntity) {
  if (entity.entityType === "artist") {
    return [
      `${entity.songCount} песен`,
      `${entity.albumCount} альбомов`,
      "Карточка summary",
    ];
  }

  if (entity.entityType === "song") {
    return [
      entity.year ? String(entity.year) : "Без даты",
      `${entity.artistCount} артистов`,
      entity.durationSeconds ? `${entity.durationSeconds} сек.` : "Длительность неизвестна",
    ];
  }

  return [
    entity.year ? String(entity.year) : "Без даты",
    `${entity.trackCount} треков`,
    entity.catalogNumber ? `Cat. ${entity.catalogNumber}` : "Без каталожного номера",
  ];
}

export function buildEntityMap<T extends LinkedEntity>(items: T[]) {
  return new Map(items.map((item) => [item.id, item]));
}

export function buildMixedEntityMap(...groups: LinkedEntity[][]) {
  return new Map<string, LinkedEntity>(groups.flatMap((items) => items.map((item) => [item.id, item] as const)));
}

export function linkedEntities<T extends LinkedEntity>(ids: string[], source: Map<string, T>) {
  return ids.map((id) => source.get(id)).filter((item): item is T => Boolean(item));
}

export function catalogPath(entityType: EntityType, pageNumber = 1) {
  const basePath = `/${entityType === "artist" ? "artists" : entityType === "song" ? "songs" : "albums"}/`;
  if (pageNumber <= 1) {
    return basePath;
  }
  return `${basePath}${pageNumber}/`;
}
