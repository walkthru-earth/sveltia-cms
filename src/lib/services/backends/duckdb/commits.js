/**
 * DuckDB backend commit operations.
 * Handles saving changes to cloud storage as Parquet files.
 * @module commits
 */

import { get } from 'svelte/store';
import { parse as parseYAML } from 'yaml';

import { getConnection } from '$lib/services/backends/duckdb/init';
import { getPresignedUrl } from '$lib/services/backends/duckdb/credentials';
import { repository, getFullStoragePath } from '$lib/services/backends/duckdb/repository';
import {
  upsertEntry,
  deleteEntry,
  exportEntriesToParquet,
} from '$lib/services/backends/duckdb/queries/entries';
import {
  insertAsset,
  deleteAsset,
  exportAssetsToParquet,
} from '$lib/services/backends/duckdb/queries/assets';
import { prefs } from '$lib/services/user/prefs';

/**
 * Parse frontmatter content from a markdown file.
 * @param {string} data Raw file content with frontmatter.
 * @returns {{ frontmatter: Record<string, any>, body: string }} Parsed frontmatter and body.
 */
function parseFrontmatterContent(data) {
  // Match frontmatter delimited by ---
  const match = data.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);

  if (!match) {
    // No frontmatter, treat entire content as body
    return { frontmatter: {}, body: data };
  }

  const [, frontmatterStr, body] = match;

  try {
    const frontmatter = parseYAML(frontmatterStr) || {};

    return { frontmatter, body: body.trim() };
  } catch (error) {
    // eslint-disable-next-line no-console
    console.warn('Failed to parse frontmatter:', error);

    return { frontmatter: {}, body: data };
  }
}

/**
 * Convert a FileChange with `data` (file content) to an Entry object.
 * @param {FileChange} change File change with data property.
 * @param {string} collectionName Collection name.
 * @returns {Entry} Entry object for database storage.
 */
function fileChangeToEntry(change, collectionName) {
  const { frontmatter, body } = parseFrontmatterContent(change.data || '');

  // Merge frontmatter with body content
  const content = { ...frontmatter };

  if (body) {
    content.body = body;
  }

  // Create entry object matching schema.js structure
  /** @type {Entry} */
  const entry = {
    id: change.slug || change.path.replace(/\.[^.]+$/, '').split('/').pop() || '',
    sha: change.previousSha || '',
    slug: change.slug || '',
    subPath: change.path,
    locales: {
      _default: {
        slug: change.slug || '',
        path: change.path,
        content,
      },
    },
  };

  return entry;
}

/**
 * @import { CommitOptions, CommitResults, FileChange, Entry, Asset } from '$lib/types/private';
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
    console.info(`[DuckDB:Commits] ${message}`, ...args);
  }
};

/**
 * Generate a SHA-256 hash for commit identification.
 * Creates a 40-character hex string similar to Git commit hashes.
 * @param {Uint8Array} content Content to hash.
 * @returns {Promise<string>} 40-character SHA hash.
 */
async function generateSha(content) {
  try {
    const hashBuffer = await crypto.subtle.digest('SHA-256', content);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');

    // Return 40 characters like Git SHA-1 (even though this is SHA-256)
    return hashHex.substring(0, 40);
  } catch (error) {
    throw new Error(`Failed to generate SHA: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Upload a Parquet file to cloud storage using a presigned PUT URL.
 * @param {string} path Storage path relative to data directory.
 * @param {Uint8Array} content Parquet file content.
 * @returns {Promise<void>}
 * @throws {Error} If upload fails or presigned URL cannot be obtained.
 */
async function uploadParquetFile(path, content) {
  debugLog(`Uploading Parquet file to ${path} (${content.length} bytes)...`);

  try {
    // Get presigned PUT URL
    const presignedUrl = await getPresignedUrl(path, 'PUT', 'application/octet-stream');

    // Upload file
    const response = await fetch(presignedUrl, {
      method: 'PUT',
      body: content,
      headers: {
        'Content-Type': 'application/octet-stream',
      },
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');

      throw new Error(`Upload failed (${response.status}): ${errorText}`);
    }

    debugLog(`Successfully uploaded ${path}`);
  } catch (error) {
    throw new Error(
      `Failed to upload ${path}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Upload an asset file to external storage using a presigned PUT URL.
 * Used when asset_mode is 'external' to store large assets separately from Parquet.
 * @param {string} path Storage path for the asset.
 * @param {Blob | Uint8Array} content Asset content.
 * @param {string} mimeType MIME type of the asset.
 * @returns {Promise<void>}
 * @throws {Error} If upload fails.
 */
async function uploadExternalAsset(path, content, mimeType) {
  debugLog(`Uploading external asset to ${path} (${mimeType})...`);

  try {
    // Get presigned PUT URL with correct content type
    const presignedUrl = await getPresignedUrl(path, 'PUT', mimeType);

    // Upload asset
    const response = await fetch(presignedUrl, {
      method: 'PUT',
      body: content,
      headers: {
        'Content-Type': mimeType,
      },
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');

      throw new Error(`Upload failed (${response.status}): ${errorText}`);
    }

    debugLog(`Successfully uploaded external asset ${path}`);
  } catch (error) {
    throw new Error(
      `Failed to upload external asset ${path}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Process entry changes (create, update, delete operations).
 * @param {AsyncDuckDBConnection} conn Database connection.
 * @param {FileChange[]} changes Array of entry changes.
 * @param {string | undefined} collectionName Collection name for the entries.
 * @returns {Promise<void>}
 */
async function processEntryChanges(conn, changes, collectionName) {
  if (changes.length === 0) {
    debugLog('No entry changes to process');

    return;
  }

  debugLog(`Processing ${changes.length} entry changes for collection: ${collectionName}`);
  // eslint-disable-next-line no-console
  console.log('[DuckDB:Commits] Entry changes:', JSON.stringify(changes, null, 2));

  for (const change of changes) {
    // eslint-disable-next-line no-console
    console.log('[DuckDB:Commits] Processing change:', change.action, 'slug:', change.slug, 'hasData:', !!change.data);

    try {
      if (change.action === 'delete') {
        // Delete entry
        if (change.slug) {
          await deleteEntry(conn, change.slug);
          debugLog(`Deleted entry: ${change.slug}`);
        }
      } else if (change.action === 'create' || change.action === 'update') {
        // Get or create entry object
        // Sveltia CMS passes `data` (file content), not `entry` objects
        const entry = change.entry || (change.data ? fileChangeToEntry(change, collectionName || '') : null);

        if (entry) {
          // eslint-disable-next-line no-console
          console.log('[DuckDB:Commits] Upserting entry:', entry.id, 'locales:', Object.keys(entry.locales || {}));
          await upsertEntry(conn, entry, collectionName || '');
          debugLog(`Upserted entry: ${entry.id} (${change.action})`);
        } else {
          // eslint-disable-next-line no-console
          console.warn('[DuckDB:Commits] No entry or data in change:', change);
        }
      } else if (change.action === 'move') {
        // For move operations, we need to delete old and insert new
        if (change.previousPath) {
          const oldSlug = change.previousPath.replace(/\.[^.]+$/, '').split('/').pop() || '';

          await deleteEntry(conn, oldSlug);
        }

        const entry = change.entry || (change.data ? fileChangeToEntry(change, collectionName || '') : null);

        if (entry) {
          await upsertEntry(conn, entry, collectionName || '');
          debugLog(`Moved entry: ${entry.id}`);
        }
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[DuckDB:Commits] Error processing change:', error);
      throw new Error(
        `Failed to process entry change (${change.action} ${change.slug || change.path}): ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  debugLog('Entry changes processed successfully');
}

/**
 * Process asset changes (upload, delete operations).
 * @param {AsyncDuckDBConnection} conn Database connection.
 * @param {FileChange[]} changes Array of asset changes.
 * @returns {Promise<void>}
 */
async function processAssetChanges(conn, changes) {
  if (changes.length === 0) {
    debugLog('No asset changes to process');

    return;
  }

  debugLog(`Processing ${changes.length} asset changes...`);

  const { assetMode } = repository;

  for (const change of changes) {
    try {
      if (change.action === 'delete') {
        // Delete asset
        if (change.asset?.sha) {
          await deleteAsset(conn, change.asset.sha);
          debugLog(`Deleted asset: ${change.asset.path}`);
        }
      } else if (change.action === 'create' || change.action === 'update') {
        // Upload asset
        if (change.asset && change.data) {
          // Convert data to Uint8Array if needed
          const content =
            change.data instanceof Uint8Array
              ? change.data
              : new Uint8Array(await new Blob([change.data]).arrayBuffer());

          // For external storage mode, upload large assets separately
          if (assetMode === 'external' && content.length > 5 * 1024 * 1024) {
            // Upload to external storage
            const storagePath = getFullStoragePath(`assets/${change.asset.path}`);
            const mimeType = change.asset.kind === 'image'
              ? `image/${change.asset.path.split('.').pop()}`
              : 'application/octet-stream';

            await uploadExternalAsset(storagePath, content, mimeType);

            // Update asset with storage URL
            change.asset.blobURL = storagePath;
          }

          // Insert asset metadata (and content if inline mode)
          await insertAsset(conn, change.asset, content);
          debugLog(`Inserted asset: ${change.asset.path} (${change.action})`);
        }
      }
    } catch (error) {
      throw new Error(
        `Failed to process asset change (${change.action} ${change.asset?.path || change.path}): ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  debugLog('Asset changes processed successfully');
}

/**
 * Commit changes to cloud storage.
 * Processes all entry and asset changes, exports to Parquet, and uploads to storage.
 * @param {FileChange[]} changes Array of file changes to commit.
 * @param {CommitOptions} options Commit options.
 * @returns {Promise<CommitResults>} Commit results with SHA, date, and file information.
 * @throws {Error} If commit fails at any stage.
 */
export const commitChanges = async (changes, options) => {
  debugLog(`Starting commit with ${changes.length} changes...`);

  // eslint-disable-next-line no-console
  console.log('[DuckDB:Commits] commitChanges called with:', {
    changesCount: changes.length,
    options,
    changes: changes.map(c => ({ action: c.action, path: c.path, hasEntry: !!c.entry, hasAsset: !!c.asset })),
  });
  // eslint-disable-next-line no-console
  console.log('[DuckDB:Commits] Full first change:', changes[0]);
  // eslint-disable-next-line no-console
  console.log('[DuckDB:Commits] First change keys:', changes[0] ? Object.keys(changes[0]) : 'no changes');

  try {
    // Get database connection
    const conn = await getConnection();

    // Sveltia CMS passes file changes with 'data' (file content) not 'entry' objects
    // We need to parse file content into entry objects for DuckDB storage
    const entryChanges = changes.filter((c) => {
      // Check if this is an entry file (not an asset)
      const isEntryFile = c.path && !c.path.match(/\.(jpg|jpeg|png|gif|svg|webp|mp4|webm|pdf|doc)$/i);
      return isEntryFile && (c.data !== undefined || c.entry !== undefined);
    });
    const assetChanges = changes.filter((c) => c.asset !== undefined);

    debugLog(`Separated into ${entryChanges.length} entry changes and ${assetChanges.length} asset changes`);

    // Get collection name from options (options.collection is the collection object)
    const collectionName = options.collection?.name || '';

    // eslint-disable-next-line no-console
    console.log('[DuckDB:Commits] Collection name:', collectionName);

    // Process all changes in the database
    await processEntryChanges(conn, entryChanges, collectionName);
    await processAssetChanges(conn, assetChanges);

    // Export tables to Parquet
    debugLog('Exporting tables to Parquet...');
    const entriesParquet = await exportEntriesToParquet(conn);
    const assetsParquet = await exportAssetsToParquet(conn);

    debugLog(`Exported entries: ${entriesParquet.length} bytes, assets: ${assetsParquet.length} bytes`);

    // Upload Parquet files to storage
    const entriesPath = getFullStoragePath('entries.parquet');
    const assetsPath = getFullStoragePath('assets.parquet');

    await Promise.all([
      uploadParquetFile(entriesPath, entriesParquet),
      uploadParquetFile(assetsPath, assetsParquet),
    ]);

    // Generate commit SHA from combined Parquet data
    const combinedContent = new Uint8Array(entriesParquet.length + assetsParquet.length);
    combinedContent.set(entriesParquet, 0);
    combinedContent.set(assetsParquet, entriesParquet.length);
    const sha = await generateSha(combinedContent);

    // Build file results map
    const files = Object.fromEntries(
      changes.map((change) => [
        change.path,
        {
          sha: change.entry?.sha || change.asset?.sha || sha,
        },
      ]),
    );

    const result = {
      sha,
      date: new Date(),
      files,
    };

    debugLog('Commit completed successfully:', sha);

    return result;
  } catch (error) {
    debugLog('Commit failed:', error);

    throw new Error(
      `Failed to commit changes: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};
