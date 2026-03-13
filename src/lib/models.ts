import { z } from "zod";

export const entityTypeSchema = z.enum(["artist", "song", "album"]);
export type EntityType = z.infer<typeof entityTypeSchema>;

export const externalLinkSchema = z.object({
  url: z.string().url(),
  label: z.string(),
  kind: z.string(),
  service: z.string(),
  official: z.boolean().default(false),
});
export type ExternalLink = z.infer<typeof externalLinkSchema>;

export const baseEntitySchema = z.object({
  id: z.string(),
  entityType: entityTypeSchema,
  vocadbId: z.number().int().nonnegative(),
  slug: z.string(),
  url: z.string(),
  source: z.literal("vocadb"),
  sourceUrl: z.string().url(),
  syncedAt: z.string(),
  upstreamVersion: z.number().int().nonnegative().nullable().optional(),
  upstreamUpdatedAt: z.string().nullable().optional(),
  rawHash: z.string(),
  status: z.string().default("active"),
});

export const artistSchema = baseEntitySchema.extend({
  entityType: z.literal("artist"),
  name: z.string(),
  defaultName: z.string(),
  displayName: z.string(),
  additionalNames: z.array(z.string()),
  artistType: z.string(),
  descriptionShort: z.string().optional().default(""),
  primaryImage: z.string().nullable().optional(),
  groups: z.array(z.string()).default([]),
  voicebanks: z.array(z.string()).default([]),
  songIds: z.array(z.string()).default([]),
  albumIds: z.array(z.string()).default([]),
  relatedArtistIds: z.array(z.string()).default([]),
  externalLinks: z.array(externalLinkSchema).default([]),
});
export type Artist = z.infer<typeof artistSchema>;

export const songSchema = baseEntitySchema.extend({
  entityType: z.literal("song"),
  title: z.string(),
  defaultName: z.string(),
  displayName: z.string(),
  additionalNames: z.array(z.string()),
  artistIds: z.array(z.string()).default([]),
  albumIds: z.array(z.string()).default([]),
  vocalistIds: z.array(z.string()).default([]),
  year: z.number().int().nullable().optional(),
  publishDate: z.string().nullable().optional(),
  durationSeconds: z.number().int().nonnegative().nullable().optional(),
  songType: z.string().optional().default("Unspecified"),
  tags: z.array(z.string()).default([]),
  primaryImage: z.string().nullable().optional(),
  externalLinks: z.array(externalLinkSchema).default([]),
});
export type Song = z.infer<typeof songSchema>;

export const albumTrackSchema = z.object({
  trackNumber: z.number().int().positive().nullable().optional(),
  discNumber: z.number().int().positive().nullable().optional(),
  songId: z.string(),
  name: z.string(),
});
export type AlbumTrack = z.infer<typeof albumTrackSchema>;

export const albumSchema = baseEntitySchema.extend({
  entityType: z.literal("album"),
  title: z.string(),
  defaultName: z.string(),
  displayName: z.string(),
  additionalNames: z.array(z.string()),
  artistIds: z.array(z.string()).default([]),
  songIds: z.array(z.string()).default([]),
  tracks: z.array(albumTrackSchema).default([]),
  year: z.number().int().nullable().optional(),
  releaseDate: z.string().nullable().optional(),
  albumType: z.string().default("Album"),
  catalogNumber: z.string().nullable().optional(),
  primaryImage: z.string().nullable().optional(),
  externalLinks: z.array(externalLinkSchema).default([]),
});
export type Album = z.infer<typeof albumSchema>;

export const graphSummarySchema = z.object({
  syncedAt: z.string(),
  artistCount: z.number().int().nonnegative(),
  songCount: z.number().int().nonnegative(),
  albumCount: z.number().int().nonnegative(),
});
export type GraphSummary = z.infer<typeof graphSummarySchema>;

export const lastRunSchema = z.object({
  mode: z.enum(["bootstrap", "incremental", "full"]),
  dryRun: z.boolean(),
  startedAt: z.string(),
  finishedAt: z.string(),
  probeRequests: z.number().int().nonnegative().default(0),
  probeHits: z.number().int().nonnegative().default(0),
  fullFetches: z.number().int().nonnegative().default(0),
  requestCount: z.number().int().nonnegative(),
  rawUpdated: z.number().int().nonnegative(),
  normalizedUpdated: z.number().int().nonnegative(),
  derivedUpdated: z.number().int().nonnegative(),
  newEntities: z.number().int().nonnegative(),
  limitedEntities: z.number().int().nonnegative(),
  errors: z.array(z.string()).default([]),
});
export type LastRun = z.infer<typeof lastRunSchema>;

const modeOverrideSchema = z.object({
  maxNewEntitiesPerRun: z.number().int().min(1).optional(),
  concurrency: z.number().int().min(1).max(8).optional(),
  probeTimeoutMs: z.number().int().min(1000).optional(),
  fetchTimeoutMs: z.number().int().min(1000).optional(),
  idLookupTimeoutMs: z.number().int().min(1000).optional(),
  rootArtistDiscographyLimit: z.number().int().min(1).optional(),
  relatedArtistDiscographyLimit: z.number().int().min(1).optional(),
});

export const seedsSchema = z.object({
  rootArtistIds: z.array(z.number().int().nonnegative()).default([1]),
  maxDepth: z.number().int().min(0).default(2),
  maxNewEntitiesPerRun: z.number().int().min(1).default(200),
  allowedEntityTypes: z.array(entityTypeSchema).default(["artist", "song", "album"]),
  expandRelatedDiscographies: z.boolean().default(true),
  concurrency: z.number().int().min(1).max(8).default(2),
  probeTimeoutMs: z.number().int().min(1000).default(12000),
  fetchTimeoutMs: z.number().int().min(1000).default(30000),
  idLookupTimeoutMs: z.number().int().min(1000).default(15000),
  rootArtistDiscographyLimit: z.number().int().min(1).default(8),
  relatedArtistDiscographyLimit: z.number().int().min(1).default(4),
  retryFailedOnIncremental: z.boolean().default(true),
  storeRaw: z.boolean().default(false),
  modes: z
    .object({
      bootstrap: modeOverrideSchema.default({}),
      incremental: modeOverrideSchema.default({}),
      full: modeOverrideSchema.default({}),
    })
    .default({
      bootstrap: {},
      incremental: {},
      full: {},
    }),
});
export type Seeds = z.infer<typeof seedsSchema>;
