import { readdir, stat } from "node:fs/promises";
import path from "node:path";

const repoRoot = process.cwd();

type Budget = {
  label: string;
  actual: number;
  limit: number;
};

function readBudget(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Environment variable ${name} must be a non-negative finite number`);
  }

  return parsed;
}

async function sizeOf(targetPath: string): Promise<number> {
  try {
    const info = await stat(targetPath);
    if (info.isFile()) {
      return info.size;
    }
    if (!info.isDirectory()) {
      return 0;
    }
  } catch {
    return 0;
  }

  const entries = await readdir(targetPath, { withFileTypes: true });
  let total = 0;
  for (const entry of entries) {
    total += await sizeOf(path.join(targetPath, entry.name));
  }
  return total;
}

async function countFiles(targetPath: string): Promise<number> {
  try {
    const info = await stat(targetPath);
    if (info.isFile()) {
      return 1;
    }
    if (!info.isDirectory()) {
      return 0;
    }
  } catch {
    return 0;
  }

  const entries = await readdir(targetPath, { withFileTypes: true });
  let total = 0;
  for (const entry of entries) {
    total += await countFiles(path.join(targetPath, entry.name));
  }
  return total;
}

function formatBytes(value: number) {
  return new Intl.NumberFormat("ru-RU").format(value);
}

async function main() {
  const derivedSize = await sizeOf(path.join(repoRoot, "data", "derived"));
  const distSize = await sizeOf(path.join(repoRoot, "dist"));
  const detailCount = await countFiles(path.join(repoRoot, "data", "derived", "detail"));

  const budgets: Budget[] = [
    {
      label: "data/derived size",
      actual: derivedSize,
      limit: readBudget("CI_MAX_DERIVED_BYTES", 350 * 1024 * 1024),
    },
    {
      label: "dist size",
      actual: distSize,
      limit: readBudget("CI_MAX_DIST_BYTES", 400 * 1024 * 1024),
    },
    {
      label: "detail payload count",
      actual: detailCount,
      limit: readBudget("CI_MAX_DETAIL_FILES", 50000),
    },
  ];

  const failed = budgets.filter((budget) => budget.actual > budget.limit);
  for (const budget of budgets) {
    console.log(`${budget.label}: ${formatBytes(budget.actual)} / ${formatBytes(budget.limit)}`);
  }

  if (failed.length > 0) {
    throw new Error(
      failed
        .map((budget) => `${budget.label} exceeded budget (${budget.actual} > ${budget.limit})`)
        .join("; "),
    );
  }
}

await main();
