const API_BASE_URL = "https://vocadb.net/api";
const USER_AGENT = "MyMikuGuide Sync Bot (+https://github.com/tokuseihagane/MyMikuGuide)";

export type SyncMode = "bootstrap" | "incremental" | "full";

export type QueueItem = {
  entityType: "artist" | "song" | "album";
  vocadbId: number;
  depth: number;
  discoveredFrom: string;
  reason: string;
  knownVersion?: number | null;
  retryCount?: number;
};

type SearchItem = {
  id: number;
};

type SearchResponse<T> = {
  items: T[];
};

type EntityMeta = {
  id: number;
  version?: number;
  status?: string;
};

type RequestOptions = {
  timeoutMs?: number;
};

async function requestJson<T>(pathname: string, params?: URLSearchParams, options?: RequestOptions) {
  const url = new URL(`${API_BASE_URL}${pathname}`);

  if (params) {
    url.search = params.toString();
  }

  const response = await fetch(url, {
    signal: AbortSignal.timeout(options?.timeoutMs ?? 30000),
    headers: {
      "User-Agent": USER_AGENT,
    },
  });

  if (!response.ok) {
    throw new Error(`VocaDB request failed: ${response.status} ${response.statusText} for ${url}`);
  }

  return (await response.json()) as T;
}

async function fetchEntityMeta(entityType: "artist" | "song" | "album", id: number, options?: RequestOptions) {
  const route = entityType === "artist" ? "artists" : entityType === "song" ? "songs" : "albums";
  return requestJson<EntityMeta>(`/${route}/${id}`, undefined, options);
}

export async function fetchArtistById(id: number, options?: RequestOptions) {
  const params = new URLSearchParams({
    fields: "Names,MainPicture,ArtistLinks,Description,WebLinks",
    lang: "English",
  });
  return requestJson<Record<string, unknown>>(`/artists/${id}`, params, options);
}

export async function fetchArtistMetaById(id: number, options?: RequestOptions) {
  return fetchEntityMeta("artist", id, options);
}

export async function fetchSongById(id: number, options?: RequestOptions) {
  const params = new URLSearchParams({
    fields: "Artists,Albums,Names,PVs,Tags,MainPicture,WebLinks",
    lang: "English",
  });
  return requestJson<Record<string, unknown>>(`/songs/${id}`, params, options);
}

export async function fetchSongMetaById(id: number, options?: RequestOptions) {
  return fetchEntityMeta("song", id, options);
}

export async function fetchAlbumById(id: number, options?: RequestOptions) {
  const params = new URLSearchParams({
    fields: "Artists,Names,Tracks,WebLinks,Description,MainPicture",
    lang: "English",
  });
  return requestJson<Record<string, unknown>>(`/albums/${id}`, params, options);
}

export async function fetchAlbumMetaById(id: number, options?: RequestOptions) {
  return fetchEntityMeta("album", id, options);
}

async function fetchPagedIds(pathname: string, params: URLSearchParams, limit = 200, options?: RequestOptions) {
  const items: number[] = [];
  let start = 0;
  const pageSize = 50;

  while (items.length < limit) {
    params.set("start", String(start));
    params.set("maxEntries", String(Math.min(pageSize, limit - items.length)));
    const response = await requestJson<SearchResponse<SearchItem>>(pathname, params, options);
    const pageItems = response.items ?? [];
    if (pageItems.length === 0) {
      break;
    }

    items.push(...pageItems.map((item) => item.id));
    if (pageItems.length < pageSize) {
      break;
    }

    start += pageSize;
  }

  return items.slice(0, limit);
}

export async function fetchSongIdsByArtistId(artistId: number, limit?: number, options?: RequestOptions) {
  const params = new URLSearchParams({
    lang: "English",
  });
  params.append("artistId[]", String(artistId));
  return fetchPagedIds("/songs", params, limit, options);
}

export async function fetchAlbumIdsByArtistId(artistId: number, limit?: number, options?: RequestOptions) {
  const params = new URLSearchParams({
    lang: "English",
  });
  params.append("artistId[]", String(artistId));
  return fetchPagedIds("/albums", params, limit, options);
}
