import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { stableStringify } from "../../src/lib/json";
import type { EntityType, LastRun, Seeds } from "../../src/lib/models";
import type { DerivedExporter, FailedEntityRecord } from "./storage";

const repoRoot = process.cwd();
export const dataRoot = path.join(repoRoot, "data");
export const rawRoot = path.join(dataRoot, "raw", "vocadb");
export const normalizedRoot = path.join(dataRoot, "normalized");
export const derivedRoot = path.join(dataRoot, "derived");
export const dbRoot = path.join(dataRoot, "db");

const ensuredDirs = new Set<string>();

export async function ensureDir(targetPath: string) {
  const dir = path.dirname(targetPath);
  if (ensuredDirs.has(dir)) return;
  await mkdir(dir, { recursive: true });
  ensuredDirs.add(dir);
}

export async function readJsonFile<T>(targetPath: string, fallback: T): Promise<T> {
  try {
    const raw = await readFile(targetPath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export async function readTextFile(targetPath: string) {
  try {
    return await readFile(targetPath, "utf8");
  } catch {
    return null;
  }
}

function contentHash(data: string) {
  return createHash("sha256").update(data).digest("hex");
}

async function atomicWrite(targetPath: string, content: string) {
  const tmpPath = `${targetPath}.tmp.${process.pid}`;
  await writeFile(tmpPath, content, "utf8");
  await rename(tmpPath, targetPath);
}

export async function writeJsonFileIfChanged(targetPath: string, value: unknown, dryRun: boolean) {
  const nextContent = stableStringify(value);

  const previousRaw = await readTextFile(targetPath);
  if (previousRaw !== null) {
    if (previousRaw.length === nextContent.length && previousRaw === nextContent) {
      return false;
    }
    if (previousRaw.length > 4096 && contentHash(previousRaw) === contentHash(nextContent)) {
      return false;
    }
  }

  if (!dryRun) {
    await ensureDir(targetPath);
    await atomicWrite(targetPath, nextContent);
  }

  return true;
}

export async function listLegacyNormalizedEntities<T>(entityType: EntityType): Promise<T[]> {
  const targetDir = path.join(normalizedRoot, `${entityType}s`);
  try {
    const files = await readdir(targetDir);
    const values = await Promise.all(
      files
        .filter((file) => file.endsWith(".json"))
        .map((file) => readJsonFile<T>(path.join(targetDir, file), null as T)),
    );
    return values.filter(Boolean);
  } catch {
    return [];
  }
}

export async function loadLegacyFailedEntities() {
  return readJsonFile<FailedEntityRecord[]>(path.join(rawRoot, "meta", "failed-entities.json"), []);
}

export async function loadLegacyLastRun() {
  return readJsonFile<LastRun | null>(path.join(rawRoot, "meta", "last-run.json"), null);
}

export async function loadSeedsFile() {
  return readJsonFile<Partial<Seeds>>(path.join(rawRoot, "meta", "seeds.json"), {});
}

export function createFileDerivedExporter(baseDir: string, dryRun: boolean): DerivedExporter {
  return {
    async writeArtifact(relativePath: string, value: unknown) {
      return writeJsonFileIfChanged(path.join(baseDir, relativePath), value, dryRun);
    },
    async deleteArtifact(relativePath: string) {
      if (dryRun) {
        return false;
      }

      try {
        await rm(path.join(baseDir, relativePath));
        return true;
      } catch {
        return false;
      }
    },
  };
}
