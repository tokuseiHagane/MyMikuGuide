import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";
import { existsSync, openSync, readSync, closeSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

const derivedRoot = resolve("data/derived");
const publicRoot = resolve("public");

function serveDerivedPlugin() {
  const prefixes = ["/MyMikuGuide/derived/", "/derived/"];

  return {
    name: "serve-derived-data",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = req.url ?? "";
        const match = prefixes.find((p) => url.startsWith(p));
        if (!match) return next();

        const relativePath = decodeURIComponent(url.slice(match.length).split("?")[0]);
        const filePath = join(derivedRoot, relativePath);

        if (!existsSync(filePath)) return next();

        const content = readFileSync(filePath, "utf8");
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.end(content);
      });
    },
  };
}

function byteRangePlugin() {
  return {
    name: "byte-range-static",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = (req.url ?? "").split("?")[0];
        if (!url.endsWith(".sqlite") && !url.endsWith(".db")) return next();

        const stripped = url.startsWith("/MyMikuGuide/") ? url.slice("/MyMikuGuide".length) : url;
        const filePath = join(publicRoot, stripped);

        if (!existsSync(filePath)) return next();

        const { size } = statSync(filePath);
        const rangeHeader = req.headers.range;

        res.setHeader("Accept-Ranges", "bytes");
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Expose-Headers", "Accept-Ranges, Content-Range, Content-Length");
        res.setHeader("Content-Type", "application/octet-stream");

        if (!rangeHeader) {
          res.setHeader("Content-Length", size);
          res.statusCode = 200;
          const fd = openSync(filePath, "r");
          const buf = Buffer.alloc(Math.min(size, 65536));
          let pos = 0;
          while (pos < size) {
            const toRead = Math.min(buf.length, size - pos);
            const bytesRead = readSync(fd, buf, 0, toRead, pos);
            if (bytesRead === 0) break;
            res.write(buf.subarray(0, bytesRead));
            pos += bytesRead;
          }
          closeSync(fd);
          res.end();
          return;
        }

        const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
        if (!match) {
          res.statusCode = 416;
          res.setHeader("Content-Range", `bytes */${size}`);
          res.end();
          return;
        }

        const start = parseInt(match[1], 10);
        const end = match[2] ? parseInt(match[2], 10) : size - 1;
        const chunkSize = end - start + 1;

        const fd = openSync(filePath, "r");
        const buf = Buffer.alloc(chunkSize);
        readSync(fd, buf, 0, chunkSize, start);
        closeSync(fd);

        res.statusCode = 206;
        res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
        res.setHeader("Content-Length", chunkSize);
        res.end(buf);
      });
    },
  };
}

export default defineConfig({
  base: "/MyMikuGuide",
  output: "static",
  site: "https://tokuseihagane.github.io/MyMikuGuide/",
  vite: {
    plugins: [tailwindcss(), byteRangePlugin(), serveDerivedPlugin()],
  },
});
