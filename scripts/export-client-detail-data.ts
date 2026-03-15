import { cp, mkdir, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

const repoRoot = process.cwd();
const sourceRoot = path.join(repoRoot, "data", "derived", "detail");
const targetRoot = path.join(repoRoot, "dist", "derived", "detail");

async function main() {
  await rm(targetRoot, { recursive: true, force: true });
  await mkdir(targetRoot, { recursive: true });

  if (!existsSync(sourceRoot)) {
    console.log(`Skipped detail JSON copy because ${sourceRoot} does not exist yet.`);
    return;
  }

  await cp(sourceRoot, targetRoot, { recursive: true });
  console.log(`Copied detail JSON to ${targetRoot}`);
}

await main();
