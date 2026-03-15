import { readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import type { EntitySummary } from "../src/lib/models";

type SummaryPage = { items?: EntitySummary[] };

const repoRoot = process.cwd();
const summaryRoot = path.join(repoRoot, "data", "derived", "summary");
const dbPath = path.join(repoRoot, "data", "db", "vocadb.sqlite");
const outputPath = path.join(repoRoot, "data", "derived", "meta", "stats.json");

function getDbTotals(): { artists: number; songs: number; albums: number } | null {
  try {
    const db = new Database(dbPath, { readonly: true });
    const row = db
      .prepare<
        unknown[],
        { a: number; s: number; al: number }
      >(
        `SELECT
          (SELECT COUNT(*) FROM artists) AS a,
          (SELECT COUNT(*) FROM songs) AS s,
          (SELECT COUNT(*) FROM albums) AS al`,
      )
      .get();
    db.close();
    return row ? { artists: row.a, songs: row.s, albums: row.al } : null;
  } catch {
    return null;
  }
}

async function readJson<T>(targetPath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(targetPath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

async function loadBucket(bucket: string): Promise<EntitySummary[]> {
  const bucketRoot = path.join(summaryRoot, bucket);
  const names = await readdir(bucketRoot).catch(() => [] as string[]);
  const pages = await Promise.all(
    names
      .filter((n) => n.endsWith(".json"))
      .sort()
      .map((n) => readJson<SummaryPage>(path.join(bucketRoot, n), { items: [] })),
  );
  return pages.flatMap((p) => p.items ?? []);
}

async function main() {
  const [artists, songs, albums] = await Promise.all([
    loadBucket("artists"),
    loadBucket("songs"),
    loadBucket("albums"),
  ]);

  const dbTotals = getDbTotals();
  const totals = dbTotals ?? {
    artists: artists.length,
    songs: songs.length,
    albums: albums.length,
  };

  const MIN_YEAR = 2004;
  const yearDist: Record<string, { songs: number; albums: number }> = {};
  for (const s of songs) {
    if (s.entityType === "song" && s.year && s.year >= MIN_YEAR) {
      const y = String(s.year);
      yearDist[y] ??= { songs: 0, albums: 0 };
      yearDist[y].songs++;
    }
  }
  for (const a of albums) {
    if (a.entityType === "album" && a.year && a.year >= MIN_YEAR) {
      const y = String(a.year);
      yearDist[y] ??= { songs: 0, albums: 0 };
      yearDist[y].albums++;
    }
  }

  const tagCounts: Record<string, number> = {};
  for (const s of songs) {
    if (s.entityType === "song") {
      for (const tag of s.tags) {
        tagCounts[tag] = (tagCounts[tag] ?? 0) + 1;
      }
    }
  }
  const topTags = Object.entries(tagCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 100)
    .map(([tag, count]) => ({ tag, count }));

  const artistTypes: Record<string, number> = {};
  for (const a of artists) {
    if (a.entityType === "artist") {
      artistTypes[a.artistType] = (artistTypes[a.artistType] ?? 0) + 1;
    }
  }

  const songTypes: Record<string, number> = {};
  for (const s of songs) {
    if (s.entityType === "song") {
      songTypes[s.songType] = (songTypes[s.songType] ?? 0) + 1;
    }
  }

  const albumTypes: Record<string, number> = {};
  for (const a of albums) {
    if (a.entityType === "album") {
      albumTypes[a.albumType] = (albumTypes[a.albumType] ?? 0) + 1;
    }
  }

  const stats = {
    totals,
    yearDistribution: Object.entries(yearDist)
      .sort((a, b) => Number(a[0]) - Number(b[0]))
      .map(([year, counts]) => ({ year: Number(year), ...counts })),
    topTags,
    artistTypes: Object.entries(artistTypes)
      .sort((a, b) => b[1] - a[1])
      .map(([type, count]) => ({ type, count })),
    songTypes: Object.entries(songTypes)
      .sort((a, b) => b[1] - a[1])
      .map(([type, count]) => ({ type, count })),
    albumTypes: Object.entries(albumTypes)
      .sort((a, b) => b[1] - a[1])
      .map(([type, count]) => ({ type, count })),
    generatedAt: new Date().toISOString(),
  };

  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, JSON.stringify(stats));
  const src = dbTotals ? "sqlite" : "derived";
  console.log(`Stats written (${src}): ${totals.artists} artists, ${totals.songs} songs, ${totals.albums} albums, ${topTags.length} tags.`);
}

await main();
