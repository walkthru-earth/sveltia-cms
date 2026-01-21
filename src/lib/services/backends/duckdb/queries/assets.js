/**
 * DuckDB Asset CRUD operations for managing CMS assets with Parquet storage.
 * Supports both inline storage (content as BLOB) and external storage (URL reference).
 * @module queries/assets
 */

import { getPathInfo } from '@sveltia/utils/file';
import { getAssetKind } from '$lib/services/assets/kinds';
import { repository } from '$lib/services/backends/duckdb/repository';

/**
 * @import { Asset, AssetFolderInfo } from '$lib/types/private';
 * @import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
 */

/**
 * Default size threshold for inline storage (5MB).
 * Assets smaller than this will be stored as BLOBs in the database.
 * Assets larger than this will be stored externally with URL references.
 */
const DEFAULT_INLINE_THRESHOLD = 5 * 1024 * 1024;

/**
 * Fetch all assets from a Parquet file.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @param {string} parquetUrl - Presigned URL to assets Parquet file.
 * @returns {Promise<Asset[]>} Array of asset objects.
 * @throws {Error} If the query fails or the Parquet file is inaccessible.
 */
export const fetchAllAssets = async (conn, parquetUrl) => {
  try {
    const result = await conn.query(`
      SELECT * FROM read_parquet('${parquetUrl}')
    `);

    const rows = result.toArray();

    return rows.map((row) => mapRowToAsset(row));
  } catch (error) {
    throw new Error(`Failed to fetch assets from Parquet: ${error.message}`);
  }
};

/**
 * Insert a new asset into the database.
 * Automatically decides between inline and external storage based on size.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @param {Asset} asset - Asset object to insert.
 * @param {Uint8Array} [content] - File content (for inline storage).
 * @param {number} [threshold=5MB] - Size threshold for inline storage.
 * @returns {Promise<void>}
 * @throws {Error} If the insert operation fails.
 */
export const insertAsset = async (conn, asset, content, threshold = DEFAULT_INLINE_THRESHOLD) => {
  const { assetMode } = repository;
  const useInline = assetMode === 'inline' && content && content.length <= threshold;

  try {
    if (useInline) {
      // Store content as BLOB in database
      await conn.query(`
        INSERT INTO cms.assets (
          id,
          path,
          filename,
          mime_type,
          size,
          kind,
          content,
          sha,
          storage_mode,
          storage_url,
          created_at,
          updated_at
        ) VALUES (
          '${asset.sha}',
          '${asset.path}',
          '${asset.name}',
          '${getMimeTypeFromPath(asset.path)}',
          ${content.length},
          '${asset.kind}',
          ?::BLOB,
          '${asset.sha}',
          'inline',
          NULL,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `, [content]);
    } else {
      // Store external URL reference
      const storageUrl = asset.blobURL || asset.path;

      await conn.query(`
        INSERT INTO cms.assets (
          id,
          path,
          filename,
          mime_type,
          size,
          kind,
          content,
          sha,
          storage_mode,
          storage_url,
          created_at,
          updated_at
        ) VALUES (
          '${asset.sha}',
          '${asset.path}',
          '${asset.name}',
          '${getMimeTypeFromPath(asset.path)}',
          ${asset.size},
          '${asset.kind}',
          NULL,
          '${asset.sha}',
          'external',
          '${storageUrl}',
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `);
    }
  } catch (error) {
    throw new Error(`Failed to insert asset: ${error.message}`);
  }
};

/**
 * Update an existing asset.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @param {string} id - Asset ID (SHA) to update.
 * @param {Partial<Asset>} updates - Fields to update.
 * @param {Uint8Array} [content] - New file content (for inline storage).
 * @returns {Promise<void>}
 * @throws {Error} If the update operation fails.
 */
export const updateAsset = async (conn, id, updates, content) => {
  try {
    const setClauses = [];
    const params = [];

    if (updates.name) {
      setClauses.push(`filename = '${updates.name}'`);
    }

    if (updates.path) {
      setClauses.push(`path = '${updates.path}'`);
    }

    if (updates.size !== undefined) {
      setClauses.push(`size = ${updates.size}`);
    }

    if (content) {
      setClauses.push('content = ?::BLOB');
      params.push(content);
      setClauses.push("storage_mode = 'inline'");
      setClauses.push('storage_url = NULL');
    } else if (updates.blobURL) {
      setClauses.push(`storage_url = '${updates.blobURL}'`);
      setClauses.push("storage_mode = 'external'");
      setClauses.push('content = NULL');
    }

    setClauses.push('updated_at = CURRENT_TIMESTAMP');

    if (setClauses.length === 0) {
      return;
    }

    await conn.query(
      `
      UPDATE cms.assets
      SET ${setClauses.join(', ')}
      WHERE id = '${id}'
    `,
      params,
    );
  } catch (error) {
    throw new Error(`Failed to update asset: ${error.message}`);
  }
};

/**
 * Delete an asset from the database.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @param {string} id - Asset ID (SHA) to delete.
 * @returns {Promise<void>}
 * @throws {Error} If the delete operation fails.
 */
export const deleteAsset = async (conn, id) => {
  try {
    await conn.query(`
      DELETE FROM cms.assets
      WHERE id = '${id}'
    `);
  } catch (error) {
    throw new Error(`Failed to delete asset: ${error.message}`);
  }
};

/**
 * Fetch asset content from the database.
 * Returns either inline content (BLOB) or external URL reference.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @param {string} id - Asset ID (SHA).
 * @returns {Promise<{content: Uint8Array | null, storageUrl: string | null, storageMode: string}>}
 * Asset content and storage information.
 * @throws {Error} If the query fails or asset is not found.
 */
export const fetchAssetContent = async (conn, id) => {
  try {
    const result = await conn.query(`
      SELECT content, storage_url, storage_mode
      FROM cms.assets
      WHERE id = '${id}'
    `);

    const rows = result.toArray();

    if (rows.length === 0) {
      throw new Error(`Asset not found: ${id}`);
    }

    const row = rows[0];

    return {
      content: row.content || null,
      storageUrl: row.storage_url || null,
      storageMode: row.storage_mode || 'external',
    };
  } catch (error) {
    throw new Error(`Failed to fetch asset content: ${error.message}`);
  }
};

/**
 * Export all assets to a Parquet blob.
 * Used when committing changes to write the updated asset table back to storage.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @returns {Promise<Uint8Array>} Parquet file as binary data.
 * @throws {Error} If the export operation fails.
 */
export const exportAssetsToParquet = async (conn) => {
  try {
    // First, create a temporary Parquet file in DuckDB's memory
    await conn.query(`
      COPY (SELECT * FROM cms.assets)
      TO '__temp_assets.parquet' (FORMAT PARQUET, COMPRESSION 'zstd')
    `);

    // Read the Parquet file as binary data
    const result = await conn.query(`
      SELECT content FROM read_blob('__temp_assets.parquet')
    `);

    const rows = result.toArray();

    if (rows.length === 0) {
      throw new Error('Failed to read exported Parquet file');
    }

    return rows[0].content;
  } catch (error) {
    throw new Error(`Failed to export assets to Parquet: ${error.message}`);
  }
};

/**
 * Initialize the assets table schema in DuckDB.
 * Creates the cms.assets table if it doesn't exist.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @returns {Promise<void>}
 * @throws {Error} If schema creation fails.
 */
export const initializeAssetsTable = async (conn) => {
  try {
    // Create schema if it doesn't exist
    await conn.query('CREATE SCHEMA IF NOT EXISTS cms');

    // Create assets table with proper schema
    await conn.query(`
      CREATE TABLE IF NOT EXISTS cms.assets (
        id VARCHAR PRIMARY KEY,
        path VARCHAR NOT NULL,
        filename VARCHAR NOT NULL,
        mime_type VARCHAR,
        size BIGINT NOT NULL,
        kind VARCHAR NOT NULL,
        content BLOB,
        sha VARCHAR NOT NULL,
        storage_mode VARCHAR NOT NULL DEFAULT 'external',
        storage_url VARCHAR,
        folder_collection VARCHAR,
        folder_file VARCHAR,
        folder_internal_path VARCHAR,
        folder_public_path VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT chk_storage_mode CHECK (storage_mode IN ('inline', 'external')),
        CONSTRAINT chk_storage CHECK (
          (storage_mode = 'inline' AND content IS NOT NULL AND storage_url IS NULL) OR
          (storage_mode = 'external' AND content IS NULL AND storage_url IS NOT NULL)
        )
      )
    `);

    // Create useful indexes
    await conn.query('CREATE INDEX IF NOT EXISTS idx_assets_path ON cms.assets(path)');
    await conn.query('CREATE INDEX IF NOT EXISTS idx_assets_kind ON cms.assets(kind)');
    await conn.query(
      'CREATE INDEX IF NOT EXISTS idx_assets_folder ON cms.assets(folder_collection, folder_file)',
    );
  } catch (error) {
    throw new Error(`Failed to initialize assets table: ${error.message}`);
  }
};

/**
 * Load assets from a Parquet file into the cms.assets table.
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @param {string} parquetUrl - Presigned URL to assets Parquet file.
 * @returns {Promise<void>}
 * @throws {Error} If loading fails.
 */
export const loadAssetsFromParquet = async (conn, parquetUrl) => {
  try {
    // Clear existing data
    await conn.query('DELETE FROM cms.assets');

    // Load from Parquet file
    await conn.query(`
      INSERT INTO cms.assets
      SELECT * FROM read_parquet('${parquetUrl}')
    `);
  } catch (error) {
    throw new Error(`Failed to load assets from Parquet: ${error.message}`);
  }
};

/**
 * Get asset count by kind (for analytics/UI).
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @returns {Promise<Record<string, number>>} Map of kind to count.
 */
export const getAssetCountByKind = async (conn) => {
  try {
    const result = await conn.query(`
      SELECT kind, COUNT(*) as count
      FROM cms.assets
      GROUP BY kind
    `);

    const rows = result.toArray();
    const counts = {};

    for (const row of rows) {
      counts[row.kind] = Number(row.count);
    }

    return counts;
  } catch (error) {
    throw new Error(`Failed to get asset counts: ${error.message}`);
  }
};

/**
 * Get total storage size by mode (inline vs external).
 * @param {AsyncDuckDBConnection} conn - Database connection.
 * @returns {Promise<{inline: number, external: number}>} Storage sizes in bytes.
 */
export const getStorageSizeByMode = async (conn) => {
  try {
    const result = await conn.query(`
      SELECT
        storage_mode,
        SUM(size) as total_size
      FROM cms.assets
      GROUP BY storage_mode
    `);

    const rows = result.toArray();
    const sizes = { inline: 0, external: 0 };

    for (const row of rows) {
      sizes[row.storage_mode] = Number(row.total_size);
    }

    return sizes;
  } catch (error) {
    throw new Error(`Failed to get storage sizes: ${error.message}`);
  }
};

/**
 * Map a DuckDB row to an Asset object.
 * Converts database columns to the Asset interface used throughout Sveltia CMS.
 * @param {object} row - Database row from DuckDB query result.
 * @returns {Asset} Asset object conforming to Sveltia's Asset type.
 */
function mapRowToAsset(row) {
  const { dirname, basename } = getPathInfo(row.path);

  /** @type {AssetFolderInfo} */
  const folder = {
    collectionName: row.folder_collection || undefined,
    fileName: row.folder_file || undefined,
    internalPath: row.folder_internal_path || undefined,
    publicPath: row.folder_public_path || undefined,
    entryRelative: false,
    hasTemplateTags: false,
  };

  return {
    sha: row.sha,
    path: row.path,
    name: row.filename || basename,
    size: Number(row.size),
    kind: row.kind || getAssetKind(row.path),
    folder,
    // Optional properties
    blobURL: row.storage_mode === 'external' ? row.storage_url : undefined,
    text: row.mime_type?.startsWith('text/') && row.storage_mode === 'inline' ? undefined : undefined,
    // Metadata (would come from separate commit tracking)
    commitAuthor: undefined,
    commitDate: row.updated_at ? new Date(row.updated_at) : undefined,
  };
}

/**
 * Get MIME type from file path.
 * @param {string} path - File path.
 * @returns {string} MIME type.
 */
function getMimeTypeFromPath(path) {
  const { extension } = getPathInfo(path);
  const mimeTypes = {
    // Images
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    png: 'image/png',
    gif: 'image/gif',
    webp: 'image/webp',
    svg: 'image/svg+xml',
    // Videos
    mp4: 'video/mp4',
    webm: 'video/webm',
    // Audio
    mp3: 'audio/mpeg',
    wav: 'audio/wav',
    ogg: 'audio/ogg',
    // Documents
    pdf: 'application/pdf',
    doc: 'application/msword',
    docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    // Text
    txt: 'text/plain',
    md: 'text/markdown',
    json: 'application/json',
    // Default
  };

  return mimeTypes[extension?.toLowerCase()] || 'application/octet-stream';
}
