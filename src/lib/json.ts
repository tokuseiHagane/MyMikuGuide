import { createHash } from "node:crypto";

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortValue);
  }

  if (value && typeof value === "object") {
    return Object.keys(value as Record<string, unknown>)
      .sort()
      .reduce<Record<string, unknown>>((acc, key) => {
        acc[key] = sortValue((value as Record<string, unknown>)[key]);
        return acc;
      }, {});
  }

  return value;
}

export function stableJsonValue<T>(value: T) {
  return sortValue(value) as T;
}

export function stableStringify(value: unknown, options?: { pretty?: boolean }) {
  const stable = stableJsonValue(value);
  const spacing = options?.pretty ? 2 : 0;
  return `${JSON.stringify(stable, null, spacing)}\n`;
}

export function hashJson(value: unknown) {
  return createHash("sha256").update(stableStringify(value)).digest("hex");
}
