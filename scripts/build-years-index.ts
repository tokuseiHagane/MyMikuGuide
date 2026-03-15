import { readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import type { EntitySummary } from "../src/lib/models";

type SummaryPage = { items?: EntitySummary[] };

const repoRoot = process.cwd();
const summaryRoot = path.join(repoRoot, "data", "derived", "summary");
const outputPath = path.join(repoRoot, "data", "derived", "meta", "years-index.json");

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
  const [songs, albums] = await Promise.all([
    loadBucket("songs"),
    loadBucket("albums"),
  ]);

  const MIN_YEAR = 2004;
  const yearMap: Record<number, { songs: number; albums: number }> = {};

  for (const s of songs) {
    if (s.entityType === "song" && s.year && s.year >= MIN_YEAR) {
      yearMap[s.year] ??= { songs: 0, albums: 0 };
      yearMap[s.year].songs++;
    }
  }

  for (const a of albums) {
    if (a.entityType === "album" && a.year && a.year >= MIN_YEAR) {
      yearMap[a.year] ??= { songs: 0, albums: 0 };
      yearMap[a.year].albums++;
    }
  }

  const years = Object.entries(yearMap)
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([year, counts]) => ({
      year: Number(year),
      songs: counts.songs,
      albums: counts.albums,
      total: counts.songs + counts.albums,
    }));

  const index = {
    years,
    totalYears: years.length,
    generatedAt: new Date().toISOString(),
  };

  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, JSON.stringify(index));
  console.log(`Years index written: ${years.length} unique years.`);
}

await main();
