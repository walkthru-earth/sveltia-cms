/**
 * DuckDB Lakehouse repository information module.
 * @module repository
 */

/**
 * @import { DuckDBRepositoryInfo } from '$lib/services/backends/duckdb/types';
 */

/**
 * Placeholder for DuckDB repository/lakehouse information.
 * Unlike Git backends, DuckDB uses cloud storage instead of a Git repository.
 * @type {DuckDBRepositoryInfo}
 */
export const repository = {
  service: '',
  label: '',
  // DuckDB-specific properties
  storageType: '',
  storageProvider: '',
  bucket: '',
  dataPath: '',
  pathPrefix: '',
  credentialProxy: '',
  catalogType: '',
  catalogEndpoint: '',
  assetMode: 'inline',
  // Common properties (partially used for DuckDB)
  owner: '',
  repo: '',
  branch: '',
  repoURL: '',
  treeBaseURL: '',
  blobBaseURL: '',
  isSelfHosted: false,
  databaseName: '',
};

/**
 * Reset the repository info to its default state.
 * Called during sign-out to clear sensitive information.
 */
export const resetRepository = () => {
  Object.assign(repository, {
    service: '',
    label: '',
    storageType: '',
    storageProvider: '',
    bucket: '',
    dataPath: '',
    pathPrefix: '',
    credentialProxy: '',
    catalogType: '',
    catalogEndpoint: '',
    assetMode: 'inline',
    owner: '',
    repo: '',
    branch: '',
    repoURL: '',
    treeBaseURL: '',
    blobBaseURL: '',
    isSelfHosted: false,
    databaseName: '',
  });
};

/**
 * Generate the full storage path for a given relative path.
 * This combines the path prefix and data path (NOT the bucket - that's handled by the presigner).
 * @param {string} relativePath Path relative to the data directory.
 * @returns {string} Full storage path within the bucket.
 */
export const getFullStoragePath = (relativePath) => {
  const { pathPrefix, dataPath } = repository;
  // Note: bucket is NOT included here - it's added by the presigner
  const parts = [pathPrefix, dataPath, relativePath].filter(Boolean);

  return parts.join('/').replace(/\/+/g, '/');
};

/**
 * Generate the S3-style URI for a given path.
 * @param {string} relativePath Path relative to the data directory.
 * @returns {string} S3-style URI (s3://bucket/path).
 */
export const getStorageURI = (relativePath) => {
  const fullPath = getFullStoragePath(relativePath);

  return `s3://${fullPath}`;
};

/**
 * Parse the storage configuration from the CMS backend config.
 * @param {object} backendConfig Backend configuration from CMS config.
 * @returns {{ storageProvider: string, bucket: string, dataPath: string, pathPrefix: string }}
 * Parsed storage configuration.
 */
export const parseStorageConfig = (backendConfig) => {
  const {
    storage = {},
  } = backendConfig;

  return {
    storageProvider: storage.provider || 's3',
    bucket: storage.bucket || '',
    dataPath: storage.data_path || 'cms/data/',
    pathPrefix: storage.path_prefix || '',
  };
};

/**
 * Parse the catalog configuration from the CMS backend config.
 * @param {object} backendConfig Backend configuration from CMS config.
 * @returns {{ catalogType: string, catalogEndpoint: string }} Parsed catalog configuration.
 */
export const parseCatalogConfig = (backendConfig) => {
  const {
    catalog = {},
  } = backendConfig;

  return {
    catalogType: catalog.type || 'duckdb',
    catalogEndpoint: catalog.endpoint || '',
  };
};
