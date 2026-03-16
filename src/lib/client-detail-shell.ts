import type { Album, AlbumDetail, Artist, ArtistDetail, EntityDetail, EntitySummary, EntityType, Song, SongDetail } from "./models";
import { getEntityCountsBatch, getEntityDetailBySlug, getEntityMainBySlug, loadDbSnapshotManifest } from "./browser-sqlite";
import { withBase } from "./site-utils";

function escapeHtml(value: unknown) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function entityHref(entity: { entityType: EntityType; slug: string }) {
  const segment = entity.entityType === "artist" ? "artists" : entity.entityType === "song" ? "songs" : "albums";
  return withBase(`/${segment}/view/?slug=${encodeURIComponent(entity.slug)}`);
}

function formatDateTime(value?: string | null) {
  if (!value) {
    return "Неизвестно";
  }

  return new Date(value).toLocaleString("ru-RU");
}

function entityBadge(entity: EntitySummary) {
  if (entity.entityType === "artist") {
    return entity.artistType;
  }
  if (entity.entityType === "song") {
    return entity.songType;
  }
  return entity.albumType;
}

function entityDescription(entity: EntitySummary) {
  if (entity.entityType === "artist") {
    return entity.descriptionShort;
  }
  if (entity.entityType === "song") {
    return entity.tags.slice(0, 4).join(" • ");
  }
  return entity.trackCount > 0 ? `${entity.trackCount} треков` : "";
}

function countLabel(count: number, label: string) {
  return count > 0 ? `${count} ${label}` : `… ${label}`;
}

function entityMeta(entity: EntitySummary) {
  if (entity.entityType === "artist") {
    return [countLabel(entity.songCount, "песен"), countLabel(entity.albumCount, "альбомов")];
  }
  if (entity.entityType === "song") {
    return [
      entity.year ? String(entity.year) : "Без даты",
      countLabel(entity.artistCount, "артистов"),
      entity.durationSeconds ? `${entity.durationSeconds} сек.` : "Длительность неизвестна",
    ];
  }
  return [
    entity.year ? String(entity.year) : "Без даты",
    countLabel(entity.trackCount, "треков"),
    entity.catalogNumber ? `Cat. ${entity.catalogNumber}` : "Без каталожного номера",
  ];
}

function renderEntityCard(entity: EntitySummary) {
  const image = entity.primaryImage
    ? `<img src="${escapeHtml(entity.primaryImage)}" alt="${escapeHtml(entity.displayName)}" loading="lazy" width="240" height="150" />`
    : `<div class="entity-card__placeholder" aria-hidden="true">${escapeHtml(entity.displayName.slice(0, 1))}</div>`;
  const description = entityDescription(entity);
  const meta = entityMeta(entity)
    .map((item) => `<li>${escapeHtml(item)}</li>`)
    .join("");

  return `
    <article class="entity-card entity-card--compact" data-entity-id="${escapeHtml(entity.id)}" data-entity-type="${entity.entityType}">
      <a class="entity-card__image" href="${entityHref(entity)}" aria-label="${escapeHtml(entity.displayName)}">
        ${image}
      </a>
      <div class="entity-card__body">
        <div class="entity-card__header">
          <span class="entity-pill">${escapeHtml(entityBadge(entity))}</span>
          ${typeof entity.upstreamVersion === "number" ? `<span class="entity-pill entity-pill--muted">v${entity.upstreamVersion}</span>` : ""}
        </div>
        <h3 class="entity-card__title">
          <a href="${entityHref(entity)}">${escapeHtml(entity.displayName)}</a>
        </h3>
        ${description ? `<p class="entity-card__description">${escapeHtml(description)}</p>` : ""}
        <ul class="entity-card__meta">
          ${meta}
          <li>Обновлено: ${escapeHtml(formatDateTime(entity.syncedAt))}</li>
        </ul>
      </div>
    </article>
  `;
}

function renderExternalLinks(
  links: Array<{ url: string; label: string; service: string }> | undefined,
  emptyText: string,
) {
  if (!Array.isArray(links) || links.length === 0) {
    return `<p class="muted">${escapeHtml(emptyText)}</p>`;
  }

  return `
    <div class="link-list">
      ${links
        .slice(0, 10)
        .map(
          (link) => `
            <a href="${escapeHtml(link.url)}">
              <span>${escapeHtml(link.label)}</span>
              <span class="muted">${escapeHtml(link.service)}</span>
            </a>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderEntitySection(title: string, subtitle: string, entities: EntitySummary[] | undefined, maxItems: number) {
  if (!Array.isArray(entities) || entities.length === 0) {
    return "";
  }

  return `
    <section style="margin-top: 1rem;" class="stack">
      <div class="section-header">
        <div>
          <h2>${escapeHtml(title)}</h2>
          <p class="muted">${escapeHtml(subtitle)}</p>
        </div>
      </div>
      <div class="card-grid card-grid--compact">
        ${entities.slice(0, maxItems).map((entity) => renderEntityCard(entity)).join("")}
      </div>
    </section>
  `;
}

function renderArtist(detail: ArtistDetail) {
  const artist = detail.entity;
  return `
    <section class="panel entity-hero">
      <div class="entity-hero__media">
        ${
          artist.primaryImage
            ? `<img src="${escapeHtml(artist.primaryImage)}" alt="${escapeHtml(artist.displayName)}" />`
            : `<div class="entity-hero__fallback" aria-hidden="true">${escapeHtml(artist.displayName.slice(0, 1))}</div>`
        }
      </div>
      <div class="stack">
        <div>
          <div class="chip-list">
            <span class="chip">${escapeHtml(artist.artistType)}</span>
            ${typeof artist.upstreamVersion === "number" ? `<span class="chip">upstream v${artist.upstreamVersion}</span>` : ""}
          </div>
          <h1>${escapeHtml(artist.displayName)}</h1>
          ${
            artist.additionalNames.length > 0
              ? `<p class="muted">Другие имена: ${escapeHtml(artist.additionalNames.slice(0, 6).join(", "))}</p>`
              : ""
          }
        </div>
        ${artist.descriptionShort ? `<p>${escapeHtml(artist.descriptionShort)}</p>` : ""}
        <ul class="meta-list">
          <li>Локально синхронизирован: ${escapeHtml(formatDateTime(artist.syncedAt))}</li>
          <li>Связанных песен: ${detail.relatedSongs.length}</li>
          <li>Связанных альбомов: ${detail.relatedAlbums.length}</li>
        </ul>
        <div class="hero__actions">
          <a class="button-link" href="${escapeHtml(artist.sourceUrl)}">Открыть в VocaDB</a>
          ${artist.primaryImage ? `<a class="button-link button-link--ghost" href="${escapeHtml(artist.primaryImage)}">Открыть исходное изображение</a>` : ""}
        </div>
      </div>
    </section>
    <section style="margin-top: 1rem;" class="detail-grid">
      <article class="panel">
        <h2>Связанные сущности</h2>
        <ul class="meta-list">
          <li>Песни: ${detail.relatedSongs.length}</li>
          <li>Альбомы: ${detail.relatedAlbums.length}</li>
          <li>Артисты: ${detail.relatedArtists.length}</li>
        </ul>
      </article>
      <article class="panel">
        <h2>Внешние ссылки</h2>
        ${renderExternalLinks(artist.externalLinks, "У артиста пока нет сохранённых внешних ссылок.")}
      </article>
    </section>
    ${renderEntitySection("Песни", "Ближайшие связанные треки из локального графа.", detail.relatedSongs, 8)}
    ${renderEntitySection("Альбомы", "Связанные альбомы, попавшие в локальный снапшот.", detail.relatedAlbums, 6)}
    ${renderEntitySection("Связанные артисты", "Другие участники и связи из локального графа.", detail.relatedArtists, 6)}
  `;
}

function renderSong(detail: SongDetail) {
  const song = detail.entity;
  return `
    <section class="panel entity-hero">
      <div class="entity-hero__media">
        ${
          song.primaryImage
            ? `<img src="${escapeHtml(song.primaryImage)}" alt="${escapeHtml(song.displayName)}" />`
            : `<div class="entity-hero__fallback" aria-hidden="true">${escapeHtml(song.displayName.slice(0, 1))}</div>`
        }
      </div>
      <div class="stack">
        <div>
          <div class="chip-list">
            <span class="chip">${escapeHtml(song.songType)}</span>
            <span class="chip">${escapeHtml(song.year ?? "Без даты")}</span>
            ${typeof song.upstreamVersion === "number" ? `<span class="chip">upstream v${song.upstreamVersion}</span>` : ""}
          </div>
          <h1>${escapeHtml(song.displayName)}</h1>
          ${
            song.additionalNames.length > 0
              ? `<p class="muted">Другие имена: ${escapeHtml(song.additionalNames.slice(0, 6).join(", "))}</p>`
              : ""
          }
        </div>
        <ul class="meta-list">
          <li>Локально синхронизирована: ${escapeHtml(formatDateTime(song.syncedAt))}</li>
          <li>Связанных артистов: ${detail.relatedArtists.length}</li>
          <li>Связанных альбомов: ${detail.relatedAlbums.length}</li>
          <li>Длительность: ${escapeHtml(song.durationSeconds ? `${song.durationSeconds} сек.` : "Неизвестно")}</li>
          <li>Дата публикации: ${escapeHtml(song.publishDate ? formatDateTime(song.publishDate) : "Неизвестно")}</li>
        </ul>
        ${
          song.tags.length > 0
            ? `<div class="chip-list">${song.tags
                .slice(0, 12)
                .map((tag) => `<span class="chip">${escapeHtml(tag)}</span>`)
                .join("")}</div>`
            : ""
        }
        <div class="hero__actions">
          <a class="button-link" href="${escapeHtml(song.sourceUrl)}">Открыть в VocaDB</a>
        </div>
      </div>
    </section>
    <section style="margin-top: 1rem;" class="detail-grid">
      <article class="panel">
        <h2>Внешние ссылки</h2>
        ${renderExternalLinks(song.externalLinks, "У этой песни пока нет сохранённых внешних ссылок.")}
      </article>
    </section>
    ${renderEntitySection("Артисты", "Кто связан с этой песней внутри локального графа.", detail.relatedArtists, 8)}
    ${renderEntitySection("Альбомы", "Альбомы, в которых песня уже встречается в локальном снапшоте.", detail.relatedAlbums, 6)}
  `;
}

function renderTrackList(tracks: AlbumDetail["tracks"]) {
  if (!Array.isArray(tracks) || tracks.length === 0) {
    return `<p class="muted">Трек-лист для этого альбома не сохранён.</p>`;
  }

  return `
    <div class="link-list">
      ${tracks
        .slice(0, 14)
        .map((track) => {
          const title = `${track.discNumber ? `${track.discNumber}.` : ""}${track.trackNumber ? `${track.trackNumber} ` : ""}${track.name}`;
          if (track.song) {
            return `
              <a href="${entityHref(track.song)}">
                <span>${escapeHtml(title)}</span>
                <span class="muted">Открыть</span>
              </a>
            `;
          }
          return `<div class="chip">${escapeHtml(title)}</div>`;
        })
        .join("")}
    </div>
  `;
}

function renderAlbum(detail: AlbumDetail) {
  const album = detail.entity;
  return `
    <section class="panel entity-hero">
      <div class="entity-hero__media">
        ${
          album.primaryImage
            ? `<img src="${escapeHtml(album.primaryImage)}" alt="${escapeHtml(album.displayName)}" />`
            : `<div class="entity-hero__fallback" aria-hidden="true">${escapeHtml(album.displayName.slice(0, 1))}</div>`
        }
      </div>
      <div class="stack">
        <div>
          <div class="chip-list">
            <span class="chip">${escapeHtml(album.albumType)}</span>
            <span class="chip">${escapeHtml(album.year ?? "Без даты")}</span>
            ${typeof album.upstreamVersion === "number" ? `<span class="chip">upstream v${album.upstreamVersion}</span>` : ""}
          </div>
          <h1>${escapeHtml(album.displayName)}</h1>
          ${
            album.additionalNames.length > 0
              ? `<p class="muted">Другие имена: ${escapeHtml(album.additionalNames.slice(0, 6).join(", "))}</p>`
              : ""
          }
        </div>
        <ul class="meta-list">
          <li>Локально синхронизирован: ${escapeHtml(formatDateTime(album.syncedAt))}</li>
          <li>Связанных артистов: ${detail.relatedArtists.length}</li>
          <li>Треков: ${detail.tracks.length}</li>
          <li>Дата релиза: ${escapeHtml(album.releaseDate ? formatDateTime(album.releaseDate) : "Неизвестно")}</li>
          <li>Каталожный номер: ${escapeHtml(album.catalogNumber ?? "Не указан")}</li>
        </ul>
        <div class="hero__actions">
          <a class="button-link" href="${escapeHtml(album.sourceUrl)}">Открыть в VocaDB</a>
        </div>
      </div>
    </section>
    <section style="margin-top: 1rem;" class="detail-grid">
      <article class="panel">
        <h2>Внешние ссылки</h2>
        ${renderExternalLinks(album.externalLinks, "У этого альбома пока нет сохранённых внешних ссылок.")}
      </article>
      <article class="panel">
        <h2>Трек-лист</h2>
        ${renderTrackList(detail.tracks)}
      </article>
    </section>
    ${renderEntitySection("Артисты", "Связанные участники и авторы альбома.", detail.relatedArtists, 8)}
    ${renderEntitySection("Песни", "Треки альбома, уже присутствующие в локальном снапшоте.", detail.relatedSongs, 12)}
  `;
}

function renderEntityHeroOnly(entityType: EntityType, entity: Artist | Song | Album) {
  const image = entity.primaryImage
    ? `<img src="${escapeHtml(entity.primaryImage)}" alt="${escapeHtml(entity.displayName)}" />`
    : `<div class="entity-hero__fallback" aria-hidden="true">${escapeHtml(entity.displayName.slice(0, 1))}</div>`;

  const typeLabel =
    entityType === "artist"
      ? (entity as Artist).artistType
      : entityType === "song"
        ? (entity as Song).songType
        : (entity as Album).albumType;

  return `
    <section class="panel entity-hero">
      <div class="entity-hero__media">${image}</div>
      <div class="stack">
        <div>
          <div class="chip-list">
            <span class="chip">${escapeHtml(typeLabel)}</span>
            ${typeof entity.upstreamVersion === "number" ? `<span class="chip">upstream v${entity.upstreamVersion}</span>` : ""}
          </div>
          <h1>${escapeHtml(entity.displayName)}</h1>
          ${
            entity.additionalNames.length > 0
              ? `<p class="muted">Другие имена: ${escapeHtml(entity.additionalNames.slice(0, 6).join(", "))}</p>`
              : ""
          }
        </div>
        <div class="hero__actions">
          <a class="button-link" href="${escapeHtml(entity.sourceUrl)}">Открыть в VocaDB</a>
        </div>
      </div>
    </section>
    <section class="panel" data-related-placeholder>
      <p class="muted">Загрузка связанных сущностей…</p>
    </section>
  `;
}

function renderDetail(detail: EntityDetail) {
  if (detail.entityType === "artist") {
    return renderArtist(detail);
  }
  if (detail.entityType === "song") {
    return renderSong(detail);
  }
  return renderAlbum(detail);
}

async function loadDetailFromJson(detailRoot: string, slug: string) {
  const response = await fetch(`${detailRoot}${encodeURIComponent(slug)}.json`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return (await response.json()) as EntityDetail;
}

async function loadDetailFromBrowserSqlite(entityType: EntityType, slug: string): Promise<EntityDetail | null> {
  if (entityType === "artist") {
    return getEntityDetailBySlug("artist", slug);
  }
  if (entityType === "song") {
    return getEntityDetailBySlug("song", slug);
  }
  return getEntityDetailBySlug("album", slug);
}

function renderMissing(shell: HTMLElement, message: string) {
  shell.innerHTML = `
    <section class="panel stack">
      <div class="section-header">
        <div>
          <h1>Карточка недоступна</h1>
          <p class="muted">${escapeHtml(message)}</p>
        </div>
        <a class="button-link button-link--ghost" href="${withBase("/search/")}">Поиск по каталогу</a>
      </div>
    </section>
  `;
}

async function hydrateVisibleCounts(container: HTMLElement) {
  const cards = container.querySelectorAll<HTMLElement>("[data-entity-id][data-entity-type]");
  if (cards.length === 0) return;

  const entities: Array<{ entityType: EntityType; id: string }> = [];
  for (const card of cards) {
    const id = card.dataset.entityId;
    const type = card.dataset.entityType as EntityType | undefined;
    if (id && type) {
      entities.push({ entityType: type, id });
    }
  }

  if (entities.length === 0) return;

  try {
    const counts = await getEntityCountsBatch(entities);

    for (const card of cards) {
      const id = card.dataset.entityId;
      const type = card.dataset.entityType as EntityType | undefined;
      if (!id || !type) continue;
      const entry = counts.get(id);
      if (!entry) continue;

      const metaItems = card.querySelectorAll<HTMLElement>(".entity-card__meta li");
      for (const li of metaItems) {
        const text = li.textContent ?? "";
        if (type === "artist") {
          if (text.includes("песен") && entry.songCount != null) {
            li.textContent = `${entry.songCount} песен`;
          } else if (text.includes("альбомов") && entry.albumCount != null) {
            li.textContent = `${entry.albumCount} альбомов`;
          }
        } else if (type === "song") {
          if (text.includes("артистов") && entry.artistCount != null) {
            li.textContent = `${entry.artistCount} артистов`;
          }
        } else if (type === "album") {
          if (text.includes("треков") && entry.trackCount != null) {
            li.textContent = `${entry.trackCount} треков`;
          }
        }
      }
    }
  } catch {
    // Counts hydration failed silently — placeholders remain.
  }
}

export function bootClientDetailShell() {
  const shell = document.querySelector<HTMLElement>("[data-detail-shell]");
  if (!shell) {
    return;
  }
  const shellElement = shell;

  const entityType = (shell.dataset.entityType as EntityType | undefined) ?? "artist";
  const detailRoot = shell.dataset.detailRoot ?? "";
  const slug = new URL(window.location.href).searchParams.get("slug")?.trim() ?? "";

  async function loadDetail() {
    if (!slug) {
      renderMissing(shellElement, "В URL не передан параметр slug.");
      return;
    }

    try {
      const manifest = await loadDbSnapshotManifest();
      const useSqlite = manifest.available && manifest.configUrl;

      if (useSqlite) {
        try {
          const mainEntity = await getEntityMainBySlug(entityType as "artist", slug);
          if (mainEntity) {
            document.title = `${mainEntity.displayName} | MyMikuGuide`;
            shellElement.innerHTML = renderEntityHeroOnly(entityType, mainEntity);
          }
        } catch {
          // Phase 1 failed, will try full load or JSON fallback below.
        }

        try {
          const detail = await loadDetailFromBrowserSqlite(entityType, slug);
          if (detail?.entity) {
            document.title = `${detail.entity.displayName} | MyMikuGuide`;
            shellElement.innerHTML = renderDetail(detail);
            void hydrateVisibleCounts(shellElement);
            return;
          }
        } catch {
          // Phase 2 failed — hero stays visible, try JSON fallback.
        }
      }

      if (detailRoot) {
        try {
          const detail = await loadDetailFromJson(detailRoot, slug);
          if (detail?.entity) {
            document.title = `${detail.entity.displayName} | MyMikuGuide`;
            shellElement.innerHTML = renderDetail(detail);
            void hydrateVisibleCounts(shellElement);
            return;
          }
        } catch {
          // JSON fallback also failed.
        }
      }

      const heroStillShown = shellElement.querySelector("[data-related-placeholder]");
      if (heroStillShown) {
        heroStillShown.innerHTML = `<p class="muted">Не удалось загрузить связанные сущности.</p>`;
        return;
      }

      renderMissing(shellElement, `Не удалось найти карточку для slug "${slug}".`);
    } catch {
      renderMissing(shellElement, `Не удалось загрузить карточку для slug "${slug}".`);
    }
  }

  void loadDetail();
}
