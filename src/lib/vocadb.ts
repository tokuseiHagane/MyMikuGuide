import { Agent, setGlobalDispatcher } from "undici";

const API_BASE_URL = "https://vocadb.net/api";
const USER_AGENT = "MyMikuGuide Sync Bot (+https://github.com/tokuseihagane/MyMikuGuide)";
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_MAX_RETRIES = 5;
const DEFAULT_RETRY_BASE_DELAY_MS = 1000;
const DEFAULT_RETRY_MAX_DELAY_MS = 10000;

export type SyncMode = "bootstrap" | "incremental" | "incremental-hot" | "reconcile-shard" | "full";

export type QueueItem = {
  entityType: "artist" | "song" | "album";
  vocadbId: number;
  depth: number;
  discoveredFrom: string;
  reason: string;
  knownVersion?: number | null;
  retryCount?: number;
  forcePersist?: boolean;
};

type SearchItem = {
  id: number;
  name?: string;
  defaultName?: string;
  version?: number;
  publishDate?: string;
  createDate?: string;
  releaseDate?: string;
};

type SearchResponse<T> = {
  items: T[];
  totalCount?: number;
};

type EntityMeta = {
  id: number;
  version?: number;
  status?: string;
};

type RequestOptions = {
  timeoutMs?: number;
  maxRetries?: number;
};

type EntityRoute = "artists" | "songs" | "albums";
let activeVocadbAgent: Agent | null = null;

export type CatalogPage = {
  items: SearchItem[];
  ids: number[];
  totalCount: number | null;
  start: number;
  maxResults: number;
};

function entityRoute(entityType: "artist" | "song" | "album"): EntityRoute {
  return entityType === "artist" ? "artists" : entityType === "song" ? "songs" : "albums";
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTimeoutError(error: unknown) {
  return error instanceof DOMException && error.name === "TimeoutError";
}

function isAbortError(error: unknown) {
  return error instanceof DOMException && error.name === "AbortError";
}

function isRetriableStatus(status: number) {
  return status === 408 || status === 425 || status === 429 || status === 502 || status === 503 || status === 504;
}

function parseRetryAfterMs(value: string | null) {
  if (!value) {
    return null;
  }

  const numericSeconds = Number(value);
  if (Number.isFinite(numericSeconds) && numericSeconds >= 0) {
    return numericSeconds * 1000;
  }

  const retryAt = Date.parse(value);
  if (!Number.isNaN(retryAt)) {
    return Math.max(retryAt - Date.now(), 0);
  }

  return null;
}

function retryDelayMs(attempt: number, retryAfterHeader: string | null) {
  const retryAfterMs = parseRetryAfterMs(retryAfterHeader);
  if (retryAfterMs !== null) {
    return Math.min(Math.max(retryAfterMs, DEFAULT_RETRY_BASE_DELAY_MS), DEFAULT_RETRY_MAX_DELAY_MS);
  }

  const exponential = DEFAULT_RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
  const capped = Math.min(exponential, DEFAULT_RETRY_MAX_DELAY_MS);
  const jitter = Math.floor(Math.random() * 250);
  return capped + jitter;
}

function retryLogPrefix() {
  return `[${new Date().toISOString()}]`;
}

export function configureVocadbNetworking(options?: { forceIPv4?: boolean; connections?: number }) {
  if (!options?.forceIPv4 || activeVocadbAgent) {
    return;
  }

  activeVocadbAgent = new Agent({
    connect: { family: 4 },
    connections: Math.max(options.connections ?? 8, 1),
    keepAliveTimeout: 10_000,
  });
  setGlobalDispatcher(activeVocadbAgent);
  console.log(
    `${retryLogPrefix()} VocaDB network | dispatcher=undici force_ipv4=true connections=${Math.max(options.connections ?? 8, 1)}`,
  );
}

export async function closeVocadbNetworking() {
  if (!activeVocadbAgent) {
    return;
  }

  await activeVocadbAgent.close();
  activeVocadbAgent = null;
}

async function requestJson<T>(pathname: string, params?: URLSearchParams, options?: RequestOptions) {
  const url = new URL(`${API_BASE_URL}${pathname}`);

  if (params) {
    url.search = params.toString();
  }

  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxRetries = Math.max(options?.maxRetries ?? DEFAULT_MAX_RETRIES, 0);
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const response = await fetch(url, {
        signal: AbortSignal.timeout(timeoutMs),
        headers: {
          "User-Agent": USER_AGENT,
        },
      });

      if (!response.ok) {
        const error = new Error(`VocaDB request failed: ${response.status} ${response.statusText} for ${url}`);
        if (attempt < maxRetries && isRetriableStatus(response.status)) {
          lastError = error;
          const delayMs = retryDelayMs(attempt, response.headers.get("retry-after"));
          console.warn(
            `${retryLogPrefix()} VocaDB retry | attempt=${attempt + 1}/${maxRetries + 1} status=${response.status} wait_ms=${delayMs} url=${url.pathname}${url.search}`,
          );
          await sleep(delayMs);
          continue;
        }

        throw error;
      }

      return (await response.json()) as T;
    } catch (error) {
      const retriableError = isTimeoutError(error) || isAbortError(error) || error instanceof TypeError;
      if (attempt < maxRetries && retriableError) {
        lastError = error instanceof Error ? error : new Error(String(error));
        const delayMs = retryDelayMs(attempt, null);
        console.warn(
          `${retryLogPrefix()} VocaDB retry | attempt=${attempt + 1}/${maxRetries + 1} error=${lastError.name}: ${lastError.message} wait_ms=${delayMs} url=${url.pathname}${url.search}`,
        );
        await sleep(delayMs);
        continue;
      }

      throw error;
    }
  }

  throw lastError ?? new Error(`VocaDB request failed after retries for ${url}`);
}

async function fetchEntityMeta(entityType: "artist" | "song" | "album", id: number, options?: RequestOptions) {
  return requestJson<EntityMeta>(`/${entityRoute(entityType)}/${id}`, undefined, options);
}

export async function fetchEntityTotalCount(entityType: "artist" | "song" | "album", options?: RequestOptions) {
  const params = new URLSearchParams({
    getTotalCount: "true",
    maxResults: "0",
    lang: "English",
  });
  const response = await requestJson<SearchResponse<SearchItem>>(`/${entityRoute(entityType)}`, params, options);
  return response.totalCount ?? 0;
}

export async function fetchCatalogPage(
  entityType: "artist" | "song" | "album",
  options: {
    start: number;
    maxResults: number;
    sort?: string;
    getTotalCount?: boolean;
  },
  requestOptions?: RequestOptions,
): Promise<CatalogPage> {
  const params = new URLSearchParams({
    lang: "English",
    start: String(Math.max(options.start, 0)),
    maxResults: String(Math.max(options.maxResults, 1)),
    getTotalCount: options.getTotalCount === false ? "false" : "true",
  });

  if (options.sort) {
    params.set("sort", options.sort);
  }

  const response = await requestJson<SearchResponse<SearchItem>>(`/${entityRoute(entityType)}`, params, requestOptions);
  const totalCount =
    options.getTotalCount === false
      ? null
      : typeof response.totalCount === "number"
        ? response.totalCount
        : null;
  return {
    items: response.items ?? [],
    ids: (response.items ?? []).map((item) => item.id),
    totalCount,
    start: Math.max(options.start, 0),
    maxResults: Math.max(options.maxResults, 1),
  };
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

async function fetchPagedIds(pathname: string, params: URLSearchParams, limit?: number | null, options?: RequestOptions) {
  const items: number[] = [];
  let start = 0;
  const pageSize = 50;
  const targetLimit = limit ?? Number.MAX_SAFE_INTEGER;

  while (items.length < targetLimit) {
    params.set("start", String(start));
    params.set("maxResults", String(Math.min(pageSize, targetLimit - items.length)));
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

  return limit == null ? items : items.slice(0, limit);
}

export async function fetchSongIdsByArtistId(artistId: number, limit?: number | null, options?: RequestOptions) {
  const params = new URLSearchParams({
    lang: "English",
  });
  params.append("artistId[]", String(artistId));
  return fetchPagedIds("/songs", params, limit, options);
}

export async function fetchAlbumIdsByArtistId(artistId: number, limit?: number | null, options?: RequestOptions) {
  const params = new URLSearchParams({
    lang: "English",
  });
  params.append("artistId[]", String(artistId));
  return fetchPagedIds("/albums", params, limit, options);
}
