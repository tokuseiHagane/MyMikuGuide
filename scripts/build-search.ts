import { readdir, readFile, rm } from "node:fs/promises";
import path from "node:path";
import * as pagefind from "pagefind";
import { toRomaji } from "wanakana";
import { detailShellPath } from "../src/lib/entity-paths";
import type { EntitySummary } from "../src/lib/models";

type SummaryPage = {
  items?: EntitySummary[];
};

const repoRoot = process.cwd();
const summaryRoot = path.join(repoRoot, "data", "derived", "summary");
const outputRoot = path.join(repoRoot, "dist", "pagefind");

async function readJson<T>(targetPath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(targetPath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function entityTypeLabel(entityType: EntitySummary["entityType"]) {
  if (entityType === "artist") {
    return "artist";
  }
  if (entityType === "song") {
    return "song";
  }
  return "album";
}

function stripBrokenSurrogates(str: string): string {
  return str.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "");
}

const KANA_RE = /[\u3040-\u309f\u30a0-\u30ff]/;

function romajiVariants(names: string[]): string[] {
  const variants: string[] = [];
  for (const name of names) {
    if (!KANA_RE.test(name)) continue;
    try {
      const romaji = toRomaji(name);
      if (romaji && romaji !== name && /[a-z]{2,}/i.test(romaji)) {
        variants.push(romaji);
      }
    } catch {
      // skip malformed input
    }
  }
  return variants;
}

function buildContent(item: EntitySummary) {
  const names = [item.displayName, ...item.additionalNames];
  const chunks = [...names, ...romajiVariants(names)];

  if (item.entityType === "artist") {
    chunks.push(item.artistType, item.descriptionShort, `${item.songCount} songs`, `${item.albumCount} albums`);
  } else if (item.entityType === "song") {
    chunks.push(item.songType, item.year ? String(item.year) : "", ...item.tags);
  } else {
    chunks.push(item.albumType, item.year ? String(item.year) : "", item.catalogNumber ?? "", `${item.trackCount} tracks`);
  }

  return stripBrokenSurrogates(chunks.filter(Boolean).join("\n"));
}

async function listSummaryFiles() {
  const buckets = ["artists", "songs", "albums"];
  const files: string[] = [];

  for (const bucket of buckets) {
    const bucketRoot = path.join(summaryRoot, bucket);
    const names = await readdir(bucketRoot).catch(() => []);
    for (const name of names.filter((entry) => entry.endsWith(".json")).sort()) {
      files.push(path.join(bucketRoot, name));
    }
  }

  return files;
}

async function loadAllSummaries() {
  const files = await listSummaryFiles();
  const pages = await Promise.all(files.map((file) => readJson<SummaryPage>(file, { items: [] })));
  return pages.flatMap((page) => page.items ?? []);
}

const MAX_PAGEFIND_RECORDS = 100_000;

function selectBalancedSubset(all: EntitySummary[], budget: number): EntitySummary[] {
  const byType = new Map<string, EntitySummary[]>();
  for (const item of all) {
    const list = byType.get(item.entityType) ?? [];
    list.push(item);
    byType.set(item.entityType, list);
  }

  for (const items of byType.values()) {
    items.sort((a, b) => b.syncedAt.localeCompare(a.syncedAt));
  }

  const sorted = [...byType.entries()].sort((a, b) => a[1].length - b[1].length);
  const allocations = new Map<string, number>();
  let remaining = budget;

  for (let i = 0; i < sorted.length; i++) {
    const [type, items] = sorted[i];
    const equalShare = Math.floor(remaining / (sorted.length - i));
    const take = Math.min(items.length, equalShare);
    allocations.set(type, take);
    remaining -= take;
    console.log(`  ${type}: ${take} of ${items.length}`);
  }

  const result: EntitySummary[] = [];
  for (const [type, items] of byType) {
    result.push(...items.slice(0, allocations.get(type)!));
  }
  return result;
}

async function main() {
  const allSummaries = await loadAllSummaries();
  const summaries =
    allSummaries.length > MAX_PAGEFIND_RECORDS
      ? selectBalancedSubset(allSummaries, MAX_PAGEFIND_RECORDS)
      : allSummaries;
  console.log(`Pagefind: indexing ${summaries.length} of ${allSummaries.length} records`);
  await rm(outputRoot, { recursive: true, force: true });

  const { index } = await pagefind.createIndex({
    forceLanguage: "ru",
  });
  if (!index) {
    throw new Error("Pagefind index was not created");
  }

  for (const item of summaries) {
    const result = await index.addCustomRecord({
      url: detailShellPath(item.entityType, item.slug),
      content: buildContent(item),
      language: "ru",
      meta: {
        title: item.displayName,
        type: entityTypeLabel(item.entityType),
      },
      filters: {
        entityType: [item.entityType],
        year:
          "year" in item && typeof item.year === "number"
            ? [String(item.year)]
            : [],
      },
      sort: {
        updated: item.syncedAt,
      },
    });

    if (result.errors.length > 0) {
      throw new Error(`Pagefind failed for ${item.entityType}:${item.slug}: ${result.errors.join("; ")}`);
    }
  }

  await index.writeFiles({
    outputPath: outputRoot,
  });
  await pagefind.close();

  console.log(`Pagefind index written with ${summaries.length} records.`);
}

await main();
