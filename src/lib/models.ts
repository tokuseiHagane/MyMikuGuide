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

export const entitySummaryBaseSchema = z.object({
  id: z.string(),
  entityType: entityTypeSchema,
  slug: z.string(),
  displayName: z.string(),
  additionalNames: z.array(z.string()).default([]),
  url: z.string(),
  sourceUrl: z.string().url(),
  syncedAt: z.string(),
  upstreamVersion: z.number().int().nonnegative().nullable().optional(),
  primaryImage: z.string().nullable().optional(),
});

export const artistSummarySchema = entitySummaryBaseSchema.extend({
  entityType: z.literal("artist"),
  artistType: z.string(),
  descriptionShort: z.string().default(""),
  songCount: z.number().int().nonnegative().default(0),
  albumCount: z.number().int().nonnegative().default(0),
});
export type ArtistSummary = z.infer<typeof artistSummarySchema>;

export const songSummarySchema = entitySummaryBaseSchema.extend({
  entityType: z.literal("song"),
  songType: z.string().default("Unspecified"),
  year: z.number().int().nullable().optional(),
  durationSeconds: z.number().int().nonnegative().nullable().optional(),
  artistCount: z.number().int().nonnegative().default(0),
  albumCount: z.number().int().nonnegative().default(0),
  tags: z.array(z.string()).default([]),
});
export type SongSummary = z.infer<typeof songSummarySchema>;

export const albumSummarySchema = entitySummaryBaseSchema.extend({
  entityType: z.literal("album"),
  albumType: z.string().default("Album"),
  year: z.number().int().nullable().optional(),
  catalogNumber: z.string().nullable().optional(),
  trackCount: z.number().int().nonnegative().default(0),
});
export type AlbumSummary = z.infer<typeof albumSummarySchema>;

export const entitySummarySchema = z.discriminatedUnion("entityType", [
  artistSummarySchema,
  songSummarySchema,
  albumSummarySchema,
]);
export type EntitySummary = z.infer<typeof entitySummarySchema>;

const summaryPageBaseSchema = z.object({
  pageNumber: z.number().int().min(1),
  pageSize: z.number().int().min(1),
  totalItems: z.number().int().nonnegative(),
  totalPages: z.number().int().nonnegative(),
});

export const artistSummaryPageSchema = summaryPageBaseSchema.extend({
  entityType: z.literal("artist"),
  items: z.array(artistSummarySchema),
});
export type ArtistSummaryPage = z.infer<typeof artistSummaryPageSchema>;

export const songSummaryPageSchema = summaryPageBaseSchema.extend({
  entityType: z.literal("song"),
  items: z.array(songSummarySchema),
});
export type SongSummaryPage = z.infer<typeof songSummaryPageSchema>;

export const albumSummaryPageSchema = summaryPageBaseSchema.extend({
  entityType: z.literal("album"),
  items: z.array(albumSummarySchema),
});
export type AlbumSummaryPage = z.infer<typeof albumSummaryPageSchema>;

export const entitySummaryPageSchema = z.discriminatedUnion("entityType", [
  artistSummaryPageSchema,
  songSummaryPageSchema,
  albumSummaryPageSchema,
]);
export type EntitySummaryPage = z.infer<typeof entitySummaryPageSchema>;

export const entityRouteSchema = z.object({
  id: z.string(),
  slug: z.string(),
  displayName: z.string(),
  pageNumber: z.number().int().min(1),
});
export type EntityRoute = z.infer<typeof entityRouteSchema>;

const routeManifestBaseSchema = z.object({
  pageSize: z.number().int().min(1),
  totalItems: z.number().int().nonnegative(),
  totalPages: z.number().int().nonnegative(),
});

export const artistRouteManifestSchema = routeManifestBaseSchema.extend({
  entityType: z.literal("artist"),
  items: z.array(entityRouteSchema),
});
export type ArtistRouteManifest = z.infer<typeof artistRouteManifestSchema>;

export const songRouteManifestSchema = routeManifestBaseSchema.extend({
  entityType: z.literal("song"),
  items: z.array(entityRouteSchema),
});
export type SongRouteManifest = z.infer<typeof songRouteManifestSchema>;

export const albumRouteManifestSchema = routeManifestBaseSchema.extend({
  entityType: z.literal("album"),
  items: z.array(entityRouteSchema),
});
export type AlbumRouteManifest = z.infer<typeof albumRouteManifestSchema>;

export const entityRouteManifestSchema = z.discriminatedUnion("entityType", [
  artistRouteManifestSchema,
  songRouteManifestSchema,
  albumRouteManifestSchema,
]);
export type EntityRouteManifest = z.infer<typeof entityRouteManifestSchema>;

export const albumTrackDetailSchema = albumTrackSchema.extend({
  song: songSummarySchema.nullable().optional(),
});
export type AlbumTrackDetail = z.infer<typeof albumTrackDetailSchema>;

export const artistDetailSchema = z.object({
  entityType: z.literal("artist"),
  entity: artistSchema,
  relatedArtists: z.array(artistSummarySchema).default([]),
  relatedSongs: z.array(songSummarySchema).default([]),
  relatedAlbums: z.array(albumSummarySchema).default([]),
});
export type ArtistDetail = z.infer<typeof artistDetailSchema>;

export const songDetailSchema = z.object({
  entityType: z.literal("song"),
  entity: songSchema,
  relatedArtists: z.array(artistSummarySchema).default([]),
  relatedAlbums: z.array(albumSummarySchema).default([]),
});
export type SongDetail = z.infer<typeof songDetailSchema>;

export const albumDetailSchema = z.object({
  entityType: z.literal("album"),
  entity: albumSchema,
  relatedArtists: z.array(artistSummarySchema).default([]),
  relatedSongs: z.array(songSummarySchema).default([]),
  tracks: z.array(albumTrackDetailSchema).default([]),
});
export type AlbumDetail = z.infer<typeof albumDetailSchema>;

export const entityDetailSchema = z.discriminatedUnion("entityType", [
  artistDetailSchema,
  songDetailSchema,
  albumDetailSchema,
]);
export type EntityDetail = z.infer<typeof entityDetailSchema>;

export const graphSummarySchema = z.object({
  syncedAt: z.string(),
  artistCount: z.number().int().nonnegative(),
  songCount: z.number().int().nonnegative(),
  albumCount: z.number().int().nonnegative(),
  summaryPageSize: z.number().int().min(1).default(1),
});
export type GraphSummary = z.infer<typeof graphSummarySchema>;

export const catalogTotalsSchema = z.object({
  artist: z.number().int().nonnegative(),
  song: z.number().int().nonnegative(),
  album: z.number().int().nonnegative(),
});
export type CatalogTotals = z.infer<typeof catalogTotalsSchema>;

export const catalogScanStateSchema = z.object({
  entityType: entityTypeSchema,
  nextStart: z.number().int().nonnegative(),
  pageSize: z.number().int().min(1).max(500),
  pass: z.number().int().min(1),
  initialTotals: catalogTotalsSchema.nullable(),
  latestTotals: catalogTotalsSchema.nullable(),
  pagesFetched: z.number().int().nonnegative().default(0),
  uniqueIdsSeen: z.number().int().nonnegative().default(0),
  duplicateIdsSeen: z.number().int().nonnegative().default(0),
  updatedAt: z.string(),
});
export type CatalogScanState = z.infer<typeof catalogScanStateSchema>;

export const hydrateStateSchema = z.enum(["pending", "hydrated", "failed"]);
export type HydrateState = z.infer<typeof hydrateStateSchema>;

export const catalogManifestSchema = z.object({
  entityType: entityTypeSchema,
  vocadbId: z.number().int().nonnegative(),
  displayName: z.string(),
  upstreamVersion: z.number().int().nonnegative().nullable().optional(),
  publishedAt: z.string().nullable().optional(),
  createdAt: z.string().nullable().optional(),
  releaseDate: z.string().nullable().optional(),
  lastSeenRunToken: z.string(),
  needsHydrate: z.boolean().default(false),
  hydrateState: hydrateStateSchema.default("pending"),
  hydratedAt: z.string().nullable().optional(),
  lastError: z.string().nullable().optional(),
  updatedAt: z.string(),
});
export type CatalogManifest = z.infer<typeof catalogManifestSchema>;

export const fullSyncPhaseSchema = z.enum(["scan", "hydrate"]);
export type FullSyncPhase = z.infer<typeof fullSyncPhaseSchema>;

export const fullSyncStateSchema = z.object({
  phase: fullSyncPhaseSchema,
  entityType: entityTypeSchema,
  nextStart: z.number().int().nonnegative(),
  pageSize: z.number().int().min(1).max(500),
  pass: z.number().int().min(1),
  runToken: z.string(),
  pagesFetched: z.number().int().nonnegative().default(0),
  uniqueIdsSeen: z.number().int().nonnegative().default(0),
  duplicateIdsSeen: z.number().int().nonnegative().default(0),
  hydratedCount: z.number().int().nonnegative().default(0),
  updatedAt: z.string(),
});
export type FullSyncState = z.infer<typeof fullSyncStateSchema>;

export const lastRunSchema = z.object({
  mode: z.enum(["bootstrap", "incremental", "incremental-hot", "reconcile-shard", "full", "derive"]),
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
  catalogPagesFetched: z.number().int().nonnegative().default(0),
  catalogUniqueIdsSeen: z.number().int().nonnegative().default(0),
  catalogDuplicateIdsSeen: z.number().int().nonnegative().default(0),
  catalogPassesCompleted: z.number().int().nonnegative().default(0),
  errors: z.array(z.string()).default([]),
});
export type LastRun = z.infer<typeof lastRunSchema>;

const optionalLimitSchema = z.number().int().min(1).nullable().optional();

const modeOverrideSchema = z.object({
  maxNewEntitiesPerRun: optionalLimitSchema,
  concurrency: z.number().int().min(1).max(64).optional(),
  catalogScanConcurrency: z.number().int().min(1).max(64).optional(),
  probeTimeoutMs: z.number().int().min(1000).optional(),
  fetchTimeoutMs: z.number().int().min(1000).optional(),
  idLookupTimeoutMs: z.number().int().min(1000).optional(),
  rootArtistDiscographyLimit: optionalLimitSchema,
  relatedArtistDiscographyLimit: optionalLimitSchema,
  hotEntityLimit: optionalLimitSchema,
  reconcileShardSize: z.number().int().min(1).max(5000).optional(),
  reconcileBucketCount: z.number().int().min(1).max(4096).optional(),
  catalogPageSize: z.number().int().min(1).max(500).optional(),
  catalogValidationPasses: z.number().int().min(0).max(8).optional(),
});

export const seedsSchema = z.object({
  rootArtistIds: z.array(z.number().int().nonnegative()).default([1]),
  maxDepth: z.number().int().min(0).default(2),
  maxNewEntitiesPerRun: z.number().int().min(1).nullable().default(null),
  allowedEntityTypes: z.array(entityTypeSchema).default(["artist", "song", "album"]),
  expandRelatedDiscographies: z.boolean().default(true),
  concurrency: z.number().int().min(1).max(64).default(32),
  catalogScanConcurrency: z.number().int().min(1).max(64).default(32),
  probeTimeoutMs: z.number().int().min(1000).default(12000),
  fetchTimeoutMs: z.number().int().min(1000).default(30000),
  idLookupTimeoutMs: z.number().int().min(1000).default(15000),
  rootArtistDiscographyLimit: z.number().int().min(1).nullable().default(null),
  relatedArtistDiscographyLimit: z.number().int().min(1).nullable().default(null),
  hotEntityLimit: z.number().int().min(1).nullable().default(250),
  reconcileShardSize: z.number().int().min(1).max(5000).default(250),
  reconcileBucketCount: z.number().int().min(1).max(4096).default(32),
  catalogPageSize: z.number().int().min(1).max(500).default(100),
  catalogValidationPasses: z.number().int().min(0).max(8).default(1),
  retryFailedOnIncremental: z.boolean().default(true),
  storeRaw: z.boolean().default(false),
  modes: z
    .object({
      bootstrap: modeOverrideSchema.default({}),
      incremental: modeOverrideSchema.default({}),
      "incremental-hot": modeOverrideSchema.default({}),
      "reconcile-shard": modeOverrideSchema.default({}),
      full: modeOverrideSchema.default({}),
    })
    .default({
      bootstrap: {},
      incremental: {},
      "incremental-hot": {},
      "reconcile-shard": {},
      full: {},
    }),
});
export type Seeds = z.infer<typeof seedsSchema>;
