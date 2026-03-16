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
 */

const CHUNK_RE = /\/sqlite\/[^/]+\/[^/]+\.\d+$/;

/** @type {Map<string, ArrayBuffer>} */
const bufferCache = new Map();

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

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
    const res = await fetch(url.href, { cache: "force-cache" });
    if (!res.ok) return res;
    bufferCache.set(cacheKey, await res.arrayBuffer());
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
