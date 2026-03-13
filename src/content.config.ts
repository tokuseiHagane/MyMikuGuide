import { defineCollection, z } from "astro:content";
import { glob } from "astro/loaders";

const pages = defineCollection({
  loader: glob({
    base: "./content/pages",
    pattern: "**/*.md",
  }),
  schema: z.object({
    title: z.string(),
    description: z.string().optional(),
  }),
});

const legacy = defineCollection({
  loader: glob({
    base: "./content/legacy",
    pattern: "**/*.md",
  }),
  schema: z.object({
    title: z.string(),
    archivedAt: z.string().optional(),
    description: z.string().optional(),
  }),
});

export const collections = {
  legacy,
  pages,
};
