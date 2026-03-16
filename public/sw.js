/*
 * Service Worker: transparent Range-request proxy for sql.js-httpvfs.
 *
 * GitHub Pages serves every response with Content-Encoding: gzip.
 * This breaks HTTP Range requests because byte offsets refer to the
 * compressed stream, not the original file.
 *
 * This SW intercepts Range requests to SQLite chunk files, fetches
 * the full chunk (browser auto-decompresses gzip), caches it in
 * memory, and returns the correct byte slice as a 206 response.
 *
 * Also provides stats and cache management via postMessage API.
 */

const CHUNK_RE = /\/sqlite\/[^/]+\/[^/]+\.\d+$/;

/** @type {Map<string, ArrayBuffer>} */
const bufferCache = new Map();
let activeRequests = 0;
let totalFetched = 0;

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  const { type } = event.data;
  if (type === "getStats") {
    event.source.postMessage({
      type: "stats",
      cachedChunks: bufferCache.size,
      cachedBytes: [...bufferCache.values()].reduce((s, b) => s + b.byteLength, 0),
      activeRequests,
      totalFetched,
    });
  } else if (type === "clearCache") {
    bufferCache.clear();
    totalFetched = 0;
    event.source.postMessage({ type: "cacheCleared" });
  }
});

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
      activeRequests--;
      broadcast({
        type: "chunkLoaded",
        url: cacheKey,
        bytes: buf.byteLength,
        cachedChunks: bufferCache.size,
        cachedBytes: [...bufferCache.values()].reduce((s, b) => s + b.byteLength, 0),
        active: activeRequests,
        totalFetched,
      });
    } catch (err) {
      activeRequests--;
      broadcast({ type: "chunkError", url: cacheKey, error: String(err), active: activeRequests });
      throw err;
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
