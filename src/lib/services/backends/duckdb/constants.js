/**
 * DuckDB Lakehouse backend constants.
 * @module constants
 */

/**
 * Backend service name identifier.
 * @type {string}
 */
export const BACKEND_NAME = 'duckdb';

/**
 * Human-readable backend service label.
 * @type {string}
 */
export const BACKEND_LABEL = 'DuckDB Lakehouse';

/**
 * Default WASM bundle URLs from jsDelivr CDN.
 * These are used when the browser auto-selects the best bundle.
 * @type {{ mvp: { mainModule: string, mainWorker: string }, eh: { mainModule: string, mainWorker: string } }}
 * @see https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/dist/
 */
export const DEFAULT_WASM_BUNDLES = {
  mvp: {
    mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-mvp.wasm',
    mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-browser-mvp.worker.js',
  },
  eh: {
    mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-eh.wasm',
    mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-browser-eh.worker.js',
  },
};

/**
 * Required DuckDB extensions for lakehouse operations.
 * - httpfs: HTTP filesystem for remote file access
 * - parquet: Parquet file format support
 * - iceberg: Apache Iceberg table format support
 * @type {string[]}
 */
export const REQUIRED_EXTENSIONS = ['httpfs', 'parquet', 'iceberg', 'spatial'];

/**
 * Default presigned URL expiry time in seconds.
 * 15 minutes provides a reasonable balance between security and usability.
 * @type {number}
 */
export const DEFAULT_PRESIGN_EXPIRY = 900;

/**
 * Default threshold for inlining assets in bytes (5 MB).
 * Assets smaller than this threshold may be stored inline in Parquet files.
 * Assets larger than this threshold are stored externally in cloud storage.
 * @type {number}
 */
export const DEFAULT_ASSET_INLINE_THRESHOLD = 5 * 1024 * 1024;

/**
 * Supported storage types for the DuckDB backend.
 * - iceberg: Apache Iceberg table format with REST catalog
 * - ducklake: DuckLake SQL-based metadata catalog
 * - parquet: Direct Parquet file storage (simplest option)
 * @type {readonly ['iceberg', 'ducklake', 'parquet']}
 */
export const STORAGE_TYPES = /** @type {const} */ (['iceberg', 'ducklake', 'parquet']);

/**
 * Supported cloud storage providers.
 * @type {readonly ['s3', 'r2', 'gcs', 'azure']}
 */
export const STORAGE_PROVIDERS = /** @type {const} */ (['s3', 'r2', 'gcs', 'azure']);

/**
 * Supported catalog types for Iceberg tables.
 * - rest: REST catalog (e.g., Tabular, Nessie, AWS Glue)
 * - duckdb: Local DuckDB catalog stored in cloud storage
 * @type {readonly ['rest', 'duckdb']}
 */
export const CATALOG_TYPES = /** @type {const} */ (['rest', 'duckdb']);

/**
 * Asset storage modes.
 * - inline: Store small assets as base64 in Parquet files
 * - external: Store all assets as separate files in cloud storage
 * @type {readonly ['inline', 'external']}
 */
export const ASSET_MODES = /** @type {const} */ (['inline', 'external']);
