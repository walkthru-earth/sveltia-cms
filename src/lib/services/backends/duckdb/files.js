/**
 * DuckDB backend file fetching operations.
 * Handles loading entries and assets from Parquet files in cloud storage.
 * @module files
 */

import { get } from 'svelte/store';

import mime from 'mime';

import { allAssets } from '$lib/services/assets';
import { getConnection } from '$lib/services/backends/duckdb/init';
import { repository } from '$lib/services/backends/duckdb/repository';
import { getPresignedUrl } from '$lib/services/backends/duckdb/credentials';
import { initializeSchema } from '$lib/services/backends/duckdb/queries/schema';
import {
  loadEntriesFromParquet,
  fetchAllEntries,
} from '$lib/services/backends/duckdb/queries/entries';
import {
  loadAssetsFromParquet,
  fetchAssetContent,
} from '$lib/services/backends/duckdb/queries/assets';
import { allEntries, dataLoaded, dataLoadedProgress } from '$lib/services/contents';
import { prefs } from '$lib/services/user/prefs';

/**
 * @import { Asset } from '$lib/types/private';
 * @import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
 */

/**
 * Log debug information when developer mode is enabled.
 * @param {string} message Log message.
 * @param  {...any} args Additional arguments to log.
 */
const debugLog = (message, ...args) => {
  if (get(prefs).devModeEnabled) {
    // eslint-disable-next-line no-console
    console.info(`[DuckDB:Files] ${message}`, ...args);
  }
};

/**
 * Get the data file paths from repository configuration.
 * @returns {{ entriesPath: string, assetsPath: string }} File paths in cloud storage.
 */
function getDataPaths() {
  const { dataPath = 'cms/data/' } = repository;

  return {
    entriesPath: `${dataPath}entries.parquet`,
    assetsPath: `${dataPath}assets.parquet`,
  };
}

/**
 * Build a public S3 URL for reading from a public bucket.
 * This avoids presigned URL issues with HEAD requests.
 * Adds cache-busting parameter to avoid stale browser cache.
 * @param {string} path File path within the bucket.
 * @param {boolean} [cacheBust=false] Whether to add cache-busting parameter.
 * @returns {string} Public S3 URL.
 */
function getPublicS3Url(path, cacheBust = false) {
  const { bucket, storageProvider } = repository;

  let baseUrl;

  // Build the public URL based on provider
  if (storageProvider === 'r2') {
    // R2 public URLs use a different format (requires public bucket or custom domain)
    baseUrl = `https://${bucket}.r2.cloudflarestorage.com/${path}`;
  } else {
    // Default S3 virtual-hosted style URL
    baseUrl = `https://${bucket}.s3.amazonaws.com/${path}`;
  }

  // Add cache-busting parameter if requested
  if (cacheBust) {
    const separator = baseUrl.includes('?') ? '&' : '?';

    return `${baseUrl}${separator}_cb=${Date.now()}`;
  }

  return baseUrl;
}

/**
 * Check if a Parquet file exists by making a HEAD request to the public URL.
 * Uses cache-busting to avoid stale browser cache responses.
 * @param {string} path File path to check.
 * @returns {Promise<boolean>} True if the file exists.
 */
async function fileExists(path) {
  try {
    // Use cache-busting to avoid stale cache responses
    const url = getPublicS3Url(path, true);

    debugLog(`Checking if file exists: ${url}`);

    const response = await fetch(url, {
      method: 'HEAD',
      // Disable browser cache for this check
      cache: 'no-store',
    });

    return response.ok;
  } catch (error) {
    debugLog(`File does not exist or error checking: ${path}`, error);

    return false;
  }
}

/**
 * Load entries from Parquet file into DuckDB.
 * Handles missing files gracefully by starting with an empty table.
 * Uses presigned URLs with DuckDB WASM configuration to avoid HEAD request issues.
 * @param {AsyncDuckDBConnection} conn Database connection.
 * @param {string} entriesPath Path to entries Parquet file.
 * @returns {Promise<void>}
 */
async function loadEntries(conn, entriesPath) {
  debugLog('Loading entries from:', entriesPath);

  try {
    // Check if the file exists using public URL (avoids presigned URL HEAD issues)
    const exists = await fileExists(entriesPath);

    if (!exists) {
      debugLog('Entries file does not exist, starting with empty table');
      return;
    }

    // Use public URL for reading (presigned URLs have HEAD request issues with DuckDB WASM)
    // The bucket is public for reads, so we don't need presigned URLs
    // Add cache-busting to avoid stale browser cache after commits
    const publicUrl = getPublicS3Url(entriesPath, true);

    debugLog('Loading entries from public URL:', publicUrl);

    // Load entries from Parquet file
    await loadEntriesFromParquet(conn, publicUrl);

    debugLog('Entries loaded successfully');
  } catch (error) {
    // Log error but don't throw - we can continue with an empty table
    debugLog('Error loading entries:', error);
    // eslint-disable-next-line no-console
    console.warn('Failed to load entries from Parquet, starting with empty table:', error);
  }
}

/**
 * Load assets from Parquet file into DuckDB.
 * Handles missing files gracefully by starting with an empty table.
 * Uses presigned URLs with DuckDB WASM configuration to avoid HEAD request issues.
 * @param {AsyncDuckDBConnection} conn Database connection.
 * @param {string} assetsPath Path to assets Parquet file.
 * @returns {Promise<void>}
 */
async function loadAssets(conn, assetsPath) {
  debugLog('Loading assets from:', assetsPath);

  try {
    // Check if the file exists using public URL (avoids presigned URL HEAD issues)
    const exists = await fileExists(assetsPath);

    if (!exists) {
      debugLog('Assets file does not exist, starting with empty table');
      return;
    }

    // Use public URL for reading (presigned URLs have HEAD request issues with DuckDB WASM)
    // The bucket is public for reads, so we don't need presigned URLs
    // Add cache-busting to avoid stale browser cache after commits
    const publicUrl = getPublicS3Url(assetsPath, true);

    debugLog('Loading assets from public URL:', publicUrl);

    // Load assets from Parquet file
    await loadAssetsFromParquet(conn, publicUrl);

    debugLog('Assets loaded successfully');
  } catch (error) {
    // Log error but don't throw - we can continue with an empty table
    debugLog('Error loading assets:', error);
    // eslint-disable-next-line no-console
    console.warn('Failed to load assets from Parquet, starting with empty table:', error);
  }
}

/**
 * Fetch all files (entries and assets) from cloud storage.
 * Loads data from Parquet files into DuckDB and populates the Sveltia CMS stores.
 * Handles missing files gracefully by starting with empty tables.
 * @returns {Promise<void>}
 * @throws {Error} If database initialization or connection fails.
 */
export async function fetchFiles() {
  debugLog('Starting to fetch files from DuckDB lakehouse...');

  try {
    // Set loading state
    dataLoaded.set(false);
    dataLoadedProgress.set(0);

    // Get database connection
    const conn = await getConnection();

    debugLog('Database connection established');

    // Initialize schema (creates tables if they don't exist)
    await initializeSchema(conn);
    debugLog('Schema initialized');

    dataLoadedProgress.set(20);

    // Get data file paths from configuration
    const { entriesPath, assetsPath } = getDataPaths();

    debugLog('Data paths:', { entriesPath, assetsPath });

    // Load entries from Parquet (gracefully handles missing files)
    await loadEntries(conn, entriesPath);
    dataLoadedProgress.set(50);

    // Load assets from Parquet (gracefully handles missing files)
    await loadAssets(conn, assetsPath);
    dataLoadedProgress.set(70);

    // Fetch all entries from DuckDB and populate the store
    debugLog('Fetching all entries from database...');
    const entries = await fetchAllEntries(conn);
    debugLog(`Fetched ${entries.length} entries`);

    allEntries.set(entries);
    dataLoadedProgress.set(85);

    // Fetch all assets from DuckDB and populate the store
    debugLog('Fetching all assets from database...');
    // Note: fetchAllAssets in assets.js expects a parquetUrl parameter
    // We need to query directly from the table instead
    const assetsResult = await conn.query('SELECT * FROM cms.assets');
    const assetsRows = assetsResult.toArray();

    // Map rows to Asset objects using the same mapping logic from assets.js
    const assets = assetsRows.map((row) => ({
      sha: row.sha || row.id,
      path: row.path,
      name: row.filename,
      size: Number(row.size),
      kind: row.kind,
      folder: {
        collectionName: row.folder_collection || undefined,
        fileName: row.folder_file || undefined,
        internalPath: row.folder_internal_path || undefined,
        publicPath: row.folder_public_path || undefined,
        entryRelative: false,
        hasTemplateTags: false,
      },
      blobURL: row.storage_mode === 'external' ? row.storage_url : undefined,
      commitDate: row.updated_at ? new Date(row.updated_at) : undefined,
    }));

    debugLog(`Fetched ${assets.length} assets`);

    allAssets.set(assets);
    dataLoadedProgress.set(100);

    // Mark data as loaded
    dataLoaded.set(true);

    debugLog('File fetching completed successfully');
  } catch (error) {
    debugLog('Error fetching files:', error);

    // Clear loading state
    dataLoadedProgress.set(undefined);

    throw new Error(
      `Failed to fetch files from DuckDB lakehouse: ${error instanceof Error ? error.message : String(error)}`,
    );
  } finally {
    // Clear progress indicator
    dataLoadedProgress.set(undefined);
  }
}

/**
 * Fetch binary content for an asset.
 * Handles both inline storage (BLOB in database) and external storage (URL reference).
 * @param {Asset} asset Asset to retrieve the file content.
 * @returns {Promise<Blob>} Blob data.
 * @throws {Error} If asset content cannot be fetched.
 */
export async function fetchBlob(asset) {
  debugLog('Fetching blob for asset:', asset.path);

  try {
    // Get database connection
    const conn = await getConnection();

    // Fetch asset content from database
    const { content, storageUrl, storageMode } = await fetchAssetContent(conn, asset.sha);

    debugLog(`Asset storage mode: ${storageMode}`);

    // Handle inline storage (content stored as BLOB in database)
    if (storageMode === 'inline' && content) {
      debugLog('Returning inline content from database');

      // Get MIME type from file path
      const mimeType = mime.getType(asset.path) || 'application/octet-stream';

      return new Blob([content], { type: mimeType });
    }

    // Handle external storage (URL reference)
    if (storageMode === 'external' && storageUrl) {
      debugLog('Fetching external content from:', storageUrl);

      // Check if storageUrl is already a presigned URL or a path
      let url = storageUrl;

      // If it's a path (not a full URL), get a presigned URL
      if (!storageUrl.startsWith('http://') && !storageUrl.startsWith('https://')) {
        url = await getPresignedUrl(storageUrl, 'GET');
      }

      // Fetch the content from the URL
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`Failed to fetch asset from storage: ${response.status} ${response.statusText}`);
      }

      // Get MIME type from response or file path
      const contentType = response.headers.get('Content-Type');
      const mimeType = contentType || mime.getType(asset.path) || 'application/octet-stream';

      // Handle SVG and other text-based files
      if (contentType && contentType !== 'application/octet-stream') {
        return new Blob([await response.text()], { type: mimeType });
      }

      return response.blob();
    }

    // If neither inline nor external storage is available, throw an error
    throw new Error(`Asset has no content: storage mode is ${storageMode}`);
  } catch (error) {
    debugLog('Error fetching blob:', error);

    throw new Error(
      `Failed to fetch blob for asset ${asset.path}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}
