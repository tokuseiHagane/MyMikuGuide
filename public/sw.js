/*
 * Service Worker: transparent Range-request proxy for sql.js-httpvfs.
 *
 * GitHub Pages serves every response with Content-Encoding: gzip.
 * This breaks HTTP Range requests because byte offsets refer to the
 * compressed stream, not the original file.
 *
 * This SW intercepts Range requests to SQLite chunk files, fetches
 * the full chunk (browser auto-decompresses gzip), caches it in a
 * two-level cache (L1: in-memory Map, L2: persistent Cache API),
 * and returns the correct byte slice as a 206 response.
 *
 * L1 (Map)      — RAM, instant access, lost on SW restart
 * L2 (Cache API) — disk, survives restarts and navigation
 *
 * Also provides stats and cache management via postMessage API.
 */

const CHUNK_RE = /\/sqlite\/[^/]+\/[^/]+\.\d+$/;
const CACHE_NAME = "sqlite-chunks-v1";

/** @type {Map<string, ArrayBuffer>} L1 — in-memory cache */
const bufferCache = new Map();
let activeRequests = 0;
let totalFetched = 0;

self.addEventListener("install", () => self.skipWaiting());

self.addEventListener("activate", (e) => {
  e.waitUntil(
    Promise.all([
      self.clients.claim(),
      caches.keys().then((names) =>
        Promise.all(
          names
            .filter((n) => n.startsWith("sqlite-chunks-") && n !== CACHE_NAME)
            .map((n) => caches.delete(n)),
        ),
      ),
    ]),
  );
});

self.addEventListener("message", (event) => {
  const { type } = event.data;
  if (type === "getStats") {
    event.waitUntil(handleGetStats(event.source));
  } else if (type === "clearCache") {
    event.waitUntil(handleClearCache(event.source));
  }
});

/** @param {Client} client */
async function handleGetStats(client) {
  let cachedChunks = bufferCache.size;
  let cachedBytes = 0;
  for (const b of bufferCache.values()) cachedBytes += b.byteLength;

  try {
    const cache = await caches.open(CACHE_NAME);
    const keys = await cache.keys();
    if (keys.length > cachedChunks) {
      cachedChunks = keys.length;
      cachedBytes = 0;
      for (const req of keys) {
        const resp = await cache.match(req);
        if (resp) {
          cachedBytes += parseInt(resp.headers.get("X-Chunk-Size") || "0", 10);
        }
      }
    }
  } catch {
    /* Cache API unavailable — fall back to Map stats */
  }

  client.postMessage({
    type: "stats",
    cachedChunks,
    cachedBytes,
    activeRequests,
    totalFetched,
  });
}

/** @param {Client} client */
async function handleClearCache(client) {
  bufferCache.clear();
  totalFetched = 0;
  await caches.delete(CACHE_NAME);
  client.postMessage({ type: "cacheCleared" });
}

function broadcast(msg) {
  self.clients.matchAll().then((clients) => {
    for (const c of clients) c.postMessage(msg);
  });
}

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (!CHUNK_RE.test(url.pathname)) return;
  event.respondWith(handleChunk(event.request, url));
});

/**
 * @param {Request} request
 * @param {URL} url
 */
async function handleChunk(request, url) {
  const cacheKey = url.origin + url.pathname;

  if (!bufferCache.has(cacheKey)) {
    const cache = await caches.open(CACHE_NAME);
    const cached = await cache.match(cacheKey);

    if (cached) {
      bufferCache.set(cacheKey, await cached.arrayBuffer());
    } else {
      activeRequests++;
      broadcast({ type: "chunkStart", url: cacheKey, active: activeRequests });

      try {
        const res = await fetch(url.href, { cache: "force-cache" });
        if (!res.ok) {
          activeRequests--;
          broadcast({ type: "chunkError", url: cacheKey, status: res.status, active: activeRequests });
          return res;
        }
        const buf = await res.arrayBuffer();
        bufferCache.set(cacheKey, buf);
        totalFetched++;

        await cache.put(
          new Request(cacheKey),
          new Response(buf.slice(0), {
            headers: { "X-Chunk-Size": String(buf.byteLength) },
          }),
        );

        activeRequests--;
        let cachedBytes = 0;
        for (const b of bufferCache.values()) cachedBytes += b.byteLength;
        broadcast({
          type: "chunkLoaded",
          url: cacheKey,
          bytes: buf.byteLength,
          cachedChunks: bufferCache.size,
          cachedBytes,
          active: activeRequests,
          totalFetched,
        });
      } catch (err) {
        activeRequests--;
        broadcast({ type: "chunkError", url: cacheKey, error: String(err), active: activeRequests });
        throw err;
      }
    }
  }

  const buffer = bufferCache.get(cacheKey);

  if (request.method === "HEAD") {
    return new Response(null, {
      status: 200,
      headers: {
        "Content-Length": buffer.byteLength,
        "Content-Type": "application/octet-stream",
        "Accept-Ranges": "bytes",
      },
    });
  }

  const range = request.headers.get("Range");
  if (range) {
    const m = range.match(/bytes=(\d+)-(\d*)/);
    if (m) {
      const start = parseInt(m[1], 10);
      const end = m[2] ? parseInt(m[2], 10) + 1 : buffer.byteLength;
      const slice = buffer.slice(start, Math.min(end, buffer.byteLength));
      return new Response(slice, {
        status: 206,
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Range": `bytes ${start}-${start + slice.byteLength - 1}/${buffer.byteLength}`,
          "Content-Length": slice.byteLength,
        },
      });
    }
  }

  return new Response(buffer.slice(0), {
    status: 200,
    headers: {
      "Content-Type": "application/octet-stream",
      "Content-Length": buffer.byteLength,
    },
  });
}
