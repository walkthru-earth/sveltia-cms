/**
 * DuckDB Lakehouse backend service.
 * Provides a vendor-agnostic data lakehouse backend for Sveltia CMS.
 * @module duckdb
 */

import { get } from 'svelte/store';

import { signIn, signOut } from '$lib/services/backends/duckdb/auth';
import { commitChanges } from '$lib/services/backends/duckdb/commits';
import {
  BACKEND_LABEL,
  BACKEND_NAME,
  ASSET_MODES,
  STORAGE_PROVIDERS,
  STORAGE_TYPES,
} from '$lib/services/backends/duckdb/constants';
import { fetchBlob, fetchFiles } from '$lib/services/backends/duckdb/files';
import {
  parseCatalogConfig,
  parseStorageConfig,
  repository,
} from '$lib/services/backends/duckdb/repository';
import { cmsConfig } from '$lib/services/config';
import { prefs } from '$lib/services/user/prefs';

/**
 * @import { BackendService, RepositoryInfo } from '$lib/types/private';
 * @import { DuckDBRepositoryInfo } from '$lib/services/backends/duckdb/types';
 */

/**
 * Initialize the DuckDB backend.
 * Parses the CMS configuration and sets up repository information.
 * @returns {RepositoryInfo | undefined} Repository info, or nothing when the configured
 * backend is not DuckDB.
 */
export const init = () => {
  const { backend } = get(cmsConfig) ?? {};

  if (backend?.name !== BACKEND_NAME) {
    return undefined;
  }

  const {
    storage_type: storageType = 'parquet',
    credential_proxy: credentialProxy = '',
    asset_mode: assetMode = 'inline',
  } = backend;

  // Validate storage type
  if (!STORAGE_TYPES.includes(storageType)) {
    // eslint-disable-next-line no-console
    console.error(`[DuckDB] Invalid storage_type: ${storageType}. Must be one of: ${STORAGE_TYPES.join(', ')}`);

    return undefined;
  }

  // Parse storage configuration
  const { storageProvider, bucket, dataPath, pathPrefix } = parseStorageConfig(backend);

  // Validate storage provider
  if (!STORAGE_PROVIDERS.includes(storageProvider)) {
    // eslint-disable-next-line no-console
    console.error(
      `[DuckDB] Invalid storage provider: ${storageProvider}. Must be one of: ${STORAGE_PROVIDERS.join(', ')}`,
    );

    return undefined;
  }

  // Validate bucket
  if (!bucket) {
    // eslint-disable-next-line no-console
    console.error('[DuckDB] Storage bucket is required');

    return undefined;
  }

  // Validate credential proxy
  if (!credentialProxy) {
    // eslint-disable-next-line no-console
    console.error('[DuckDB] credential_proxy URL is required');

    return undefined;
  }

  // Validate asset mode
  if (!ASSET_MODES.includes(assetMode)) {
    // eslint-disable-next-line no-console
    console.error(`[DuckDB] Invalid asset_mode: ${assetMode}. Must be one of: ${ASSET_MODES.join(', ')}`);

    return undefined;
  }

  // Parse catalog configuration
  const { catalogType, catalogEndpoint } = parseCatalogConfig(backend);

  // Generate database name for IndexedDB (used for caching)
  const databaseName = `${BACKEND_NAME}:${storageProvider}:${bucket}${pathPrefix ? `:${pathPrefix}` : ''}`;

  Object.assign(
    repository,
    /** @type {DuckDBRepositoryInfo} */ ({
      service: BACKEND_NAME,
      label: BACKEND_LABEL,
      // DuckDB-specific properties
      storageType,
      storageProvider,
      bucket,
      dataPath,
      pathPrefix,
      credentialProxy,
      catalogType,
      catalogEndpoint,
      assetMode,
      // Common properties
      owner: '',
      repo: '',
      branch: '',
      repoURL: '',
      treeBaseURL: '',
      blobBaseURL: '',
      isSelfHosted: false,
      databaseName,
    }),
  );

  if (get(prefs).devModeEnabled) {
    // eslint-disable-next-line no-console
    console.info('[DuckDB] repositoryInfo', repository);
  }

  return repository;
};

// Re-export auth functions for module consumers
export { signIn, signOut };

// Re-export file operations for module consumers
export { fetchFiles, fetchBlob };

// Re-export commit operations for module consumers
export { commitChanges };

/**
 * @type {BackendService}
 */
export default {
  isGit: false,
  name: BACKEND_NAME,
  label: BACKEND_LABEL,
  repository,
  init,
  signIn,
  signOut,
  fetchFiles,
  fetchBlob,
  commitChanges,
};
