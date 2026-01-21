/**
 * DuckDB Lakehouse backend type definitions.
 * @module types
 */

/**
 * @import { RepositoryInfo } from '$lib/types/private';
 */

/**
 * Storage type for the DuckDB backend.
 * @typedef {'iceberg' | 'ducklake' | 'parquet'} DuckDBStorageType
 */

/**
 * Cloud storage provider.
 * @typedef {'s3' | 'r2' | 'gcs' | 'azure'} StorageProvider
 */

/**
 * Catalog type for Iceberg tables.
 * @typedef {'rest' | 'duckdb'} CatalogType
 */

/**
 * Asset storage mode.
 * @typedef {'inline' | 'external'} AssetMode
 */

/**
 * DuckDB-specific repository/lakehouse information.
 * Extends the base RepositoryInfo with cloud storage properties.
 * @typedef {RepositoryInfo & DuckDBRepositoryExtraProps} DuckDBRepositoryInfo
 */

/**
 * Extra properties specific to DuckDB backend.
 * @typedef {object} DuckDBRepositoryExtraProps
 * @property {DuckDBStorageType} storageType Storage type (iceberg, ducklake, parquet).
 * @property {StorageProvider} storageProvider Cloud storage provider (s3, r2, gcs, azure).
 * @property {string} bucket Cloud storage bucket name.
 * @property {string} dataPath Path within the bucket for CMS data.
 * @property {string} pathPrefix Optional path prefix for shared buckets.
 * @property {string} credentialProxy URL of the credential proxy worker.
 * @property {CatalogType} catalogType Catalog type for Iceberg tables.
 * @property {string} catalogEndpoint REST catalog endpoint URL (if using REST catalog).
 * @property {AssetMode} assetMode How assets are stored (inline or external).
 */

/**
 * Presigned URL request payload.
 * @typedef {object} PresignRequest
 * @property {'GET' | 'PUT' | 'DELETE'} operation HTTP operation for the presigned URL.
 * @property {string} path Path to the object in storage.
 * @property {string} [contentType] Content type for PUT operations.
 * @property {number} [expiresIn] Expiry time in seconds (default: 900).
 */

/**
 * Presigned URL response from the credential proxy.
 * @typedef {object} PresignResponse
 * @property {string} url The presigned URL.
 * @property {number} expiresAt Unix timestamp when the URL expires.
 * @property {Record<string, string>} [headers] Additional headers to include with the request.
 */

/**
 * Cached presigned URL with metadata.
 * @typedef {object} CachedPresignedURL
 * @property {string} url The presigned URL.
 * @property {number} expiresAt Unix timestamp when the URL expires.
 * @property {Record<string, string>} [headers] Additional headers.
 * @property {number} cachedAt Unix timestamp when the URL was cached.
 */

/**
 * DuckDB backend configuration from CMS config.
 * @typedef {object} DuckDBBackendConfig
 * @property {'duckdb'} name Backend name.
 * @property {DuckDBStorageType} [storage_type] Storage type (default: 'parquet').
 * @property {DuckDBCatalogConfig} [catalog] Catalog configuration.
 * @property {DuckDBStorageConfig} storage Storage configuration.
 * @property {string} credential_proxy URL of the credential proxy worker.
 * @property {AssetMode} [asset_mode] Asset storage mode (default: 'inline').
 * @property {'github' | 'gitlab'} [oauth_provider] OAuth provider for authentication (default: 'github').
 * @property {string} [oauth_client_id] OAuth client ID (optional, uses default if not provided).
 * @property {string} [oauth_auth_url] OAuth authorization URL (optional, uses default if not provided).
 * @property {string} [oauth_token_url] OAuth token URL (optional, uses default if not provided).
 */

/**
 * Catalog configuration for DuckDB backend.
 * @typedef {object} DuckDBCatalogConfig
 * @property {CatalogType} [type] Catalog type (default: 'duckdb').
 * @property {string} [endpoint] REST catalog endpoint URL.
 */

/**
 * Storage configuration for DuckDB backend.
 * @typedef {object} DuckDBStorageConfig
 * @property {StorageProvider} [provider] Storage provider (default: 's3').
 * @property {string} bucket Bucket name.
 * @property {string} [data_path] Path for CMS data (default: 'cms/data/').
 * @property {string} [path_prefix] Path prefix for shared buckets.
 */

/**
 * DuckDB connection state.
 * @typedef {object} DuckDBConnectionState
 * @property {boolean} initialized Whether DuckDB WASM is initialized.
 * @property {boolean} connected Whether a connection is active.
 * @property {boolean} loading Whether an operation is in progress.
 * @property {Error | null} error Last error that occurred.
 */

/**
 * Sign-in options specific to DuckDB backend.
 * @typedef {object} DuckDBSignInOptions
 * @property {boolean} auto Whether this is an automatic sign-in attempt.
 * @property {string} [sessionToken] Cached session token from previous sign-in.
 */

export {};
