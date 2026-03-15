import type {
  Album,
  Artist,
  CatalogManifest,
  CatalogScanState,
  EntitySummary,
  EntityType,
  FullSyncState,
  LastRun,
  Seeds,
  Song,
} from "../../src/lib/models";
import type { QueueItem } from "../../src/lib/vocadb";

export type NormalizedEntity = Artist | Song | Album;

export type KnownEntityRecord = {
  vocadbId: number;
  upstreamVersion: number | null;
};

export type CatalogManifestUpsertRecord = Pick<
  CatalogManifest,
  "entityType" | "vocadbId" | "displayName" | "upstreamVersion" | "publishedAt" | "createdAt" | "releaseDate" | "lastSeenRunToken" | "needsHydrate"
>;

export type ReconcileState = {
  entityType: EntityType;
  bucket: number;
  bucketCount: number;
  lastVocadbId: number;
  cycle: number;
  updatedAt: string;
};

export type FailedEntityRecord = QueueItem & {
  error: string;
  failedAt: string;
};

export type LegacyStoreSnapshot = {
  artists: Artist[];
  songs: Song[];
  albums: Album[];
  failedEntities: FailedEntityRecord[];
  lastRun: LastRun | null;
  seeds: Seeds | Omit<Seeds, "modes">;
};

export interface NormalizedStore {
  hasAnyEntities(): Promise<boolean>;
  importLegacySnapshot(snapshot: LegacyStoreSnapshot): Promise<void>;
  listEntities<T extends NormalizedEntity>(entityType: EntityType): Promise<T[]>;
  getKnownEntities(entityType: EntityType): Promise<KnownEntityRecord[]>;
  getKnownEntitiesPage(
    entityType: EntityType,
    options?: {
      afterVocadbId?: number;
      limit?: number;
      bucket?: number;
      bucketCount?: number;
    },
  ): Promise<KnownEntityRecord[]>;
  getKnownVersions(entityType: EntityType, vocadbIds: number[]): Promise<Map<number, number | null>>;
  persistEntity(entity: NormalizedEntity): Promise<{
    isNew: boolean;
    changed: boolean;
    previousEntity: NormalizedEntity | null;
  }>;
}

export interface MetaStore {
  loadFailedEntities(): Promise<FailedEntityRecord[]>;
  loadCatalogScanState(): Promise<CatalogScanState | null>;
  loadFullSyncState(): Promise<FullSyncState | null>;
  saveFailedEntities(entries: FailedEntityRecord[]): Promise<void>;
  saveCatalogScanState(state: CatalogScanState): Promise<void>;
  clearCatalogScanState(): Promise<void>;
  saveFullSyncState(state: FullSyncState): Promise<void>;
  clearFullSyncState(): Promise<void>;
  saveLastRun(lastRun: LastRun): Promise<void>;
  saveSeedsSnapshot(seeds: Seeds | Omit<Seeds, "modes">): Promise<void>;
  saveNewEntries(entries: EntitySummary[]): Promise<void>;
  upsertCatalogManifestEntries(entries: CatalogManifestUpsertRecord[]): Promise<void>;
  listHydrateCandidates(entityType: EntityType, afterVocadbId: number, limit: number): Promise<KnownEntityRecord[]>;
  markManifestHydrated(entityType: EntityType, vocadbId: number): Promise<void>;
  markManifestHydrateFailed(entityType: EntityType, vocadbId: number, error: string): Promise<void>;
  countPendingHydrates(): Promise<number>;
}

export interface DerivedExporter {
  writeArtifact(relativePath: string, value: unknown): Promise<boolean>;
  deleteArtifact(relativePath: string): Promise<boolean>;
}
