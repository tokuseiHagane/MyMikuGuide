import { readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import type { EntitySummary, SongSummary } from "../src/lib/models";

type SummaryPage = { items?: EntitySummary[] };

const repoRoot = process.cwd();
const summaryRoot = path.join(repoRoot, "data", "derived", "summary", "songs");
const outputPath = path.join(repoRoot, "data", "derived", "meta", "tags-index.json");

async function readJson<T>(targetPath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(targetPath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

async function main() {
  const names = await readdir(summaryRoot).catch(() => [] as string[]);
  const pages = await Promise.all(
    names
      .filter((n) => n.endsWith(".json"))
      .sort()
      .map((n) => readJson<SummaryPage>(path.join(summaryRoot, n), { items: [] })),
  );
  const songs = pages.flatMap((p) => p.items ?? []) as SongSummary[];

  const tagMap: Record<string, { count: number; slugs: string[] }> = {};

  for (const song of songs) {
    for (const tag of song.tags) {
      if (!tagMap[tag]) {
        tagMap[tag] = { count: 0, slugs: [] };
      }
      tagMap[tag].count++;
      if (tagMap[tag].slugs.length < 50) {
        tagMap[tag].slugs.push(song.slug);
      }
    }
  }

  const tags = Object.entries(tagMap)
    .sort((a, b) => b[1].count - a[1].count)
    .map(([tag, data]) => ({ tag, count: data.count, slugs: data.slugs }));

  const index = {
    totalTags: tags.length,
    totalSongs: songs.length,
    tags,
    generatedAt: new Date().toISOString(),
  };

  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, JSON.stringify(index));
  console.log(`Tags index written: ${tags.length} unique tags from ${songs.length} songs.`);
}

await main();
