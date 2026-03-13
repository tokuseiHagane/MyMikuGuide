const CYRILLIC_MAP: Record<string, string> = {
  а: "a",
  б: "b",
  в: "v",
  г: "g",
  д: "d",
  е: "e",
  ё: "e",
  ж: "zh",
  з: "z",
  и: "i",
  й: "y",
  к: "k",
  л: "l",
  м: "m",
  н: "n",
  о: "o",
  п: "p",
  р: "r",
  с: "s",
  т: "t",
  у: "u",
  ф: "f",
  х: "h",
  ц: "ts",
  ч: "ch",
  ш: "sh",
  щ: "sch",
  ы: "y",
  э: "e",
  ю: "yu",
  я: "ya",
  ь: "",
  ъ: "",
};

function transliterate(input: string) {
  return input
    .split("")
    .map((char) => {
      const lower = char.toLowerCase();
      return CYRILLIC_MAP[lower] ?? char;
    })
    .join("");
}

export function slugify(value: string, fallbackId?: number) {
  const base = transliterate(value)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-")
    .toLowerCase();

  if (base) {
    return fallbackId ? `${base}--vocadb-${fallbackId}` : base;
  }

  return fallbackId ? `vocadb-${fallbackId}` : "untitled";
}
