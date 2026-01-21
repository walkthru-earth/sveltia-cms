/**
 * Entry CRUD operations for DuckDB backend.
 * Provides SQL query builders and mapping functions for managing CMS entries.
 * @module queries/entries
 */

import { get } from 'svelte/store';

import { getConnection } from '$lib/services/backends/duckdb/init';
import { prefs } from '$lib/services/user/prefs';

/**
 * @import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
 * @import { Entry, LocalizedEntry, FlattenedEntryContent, CommitAuthor } from '$lib/types/private';
 */

/**
 * Note: The entries table schema is defined in schema.js and matches the Parquet file structure:
 * id, collection, slug, locale, path, data, status, sha, created_at, updated_at, created_by, updated_by
 *
 * This module uses the schema from schema.js for consistency.
 */

/**
 * Log debug information when developer mode is enabled.
 * @param {string} message Log message.
 * @param  {...any} args Additional arguments to log.
 */
const debugLog = (message, ...args) => {
  if (get(prefs).devModeEnabled) {
    // eslint-disable-next-line no-console
    console.info(`[DuckDB:Entries] ${message}`, ...args);
  }
};

/**
 * Initialize the entries table in the cms schema.
 * Uses the schema defined in schema.js for consistency.
 * @param {AsyncDuckDBConnection} conn Database connection.
 * @returns {Promise<void>}
 */
export const initEntriesTable = async (conn) => {
  debugLog('Initializing entries table...');

  try {
    // Import and use the schema from schema.js
    const { initializeSchema } = await import('$lib/services/backends/duckdb/queries/schema');
    await initializeSchema(conn);

    debugLog('Entries table initialized successfully');
  } catch (error) {
    throw new Error(
      `Failed to initialize entries table: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Load entries from a remote Parquet file into the local DuckDB table.
 * This replaces the local table content with the remote Parquet data.
 * @param {AsyncDuckDBConnection} conn Database connection.
 * @param {string} parquetUrl Presigned URL to entries Parquet file.
 * @returns {Promise<void>}
 */
export const loadEntriesFromParquet = async (conn, parquetUrl) => {
  debugLog('Loading entries from Parquet:', parquetUrl);

  try {
    // Initialize table first
    await initEntriesTable(conn);

    // Clear existing data before loading from Parquet
    await conn.query('DELETE FROM cms.entries');

    // Load data from Parquet file
    await conn.query(`
      INSERT INTO cms.entries
      SELECT * FROM read_parquet('${parquetUrl}')
    `);

    debugLog('Entries loaded successfully from Parquet');
  } catch (error) {
    throw new Error(
      `Failed to load entries from Parquet: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Map a DuckDB row to a LocalizedEntry object.
 * Uses the schema.js format: id, collection, slug, locale, path, data, status, sha, etc.
 * @param {Record<string, any>} row Database row.
 * @returns {LocalizedEntry} Localized entry object.
 */
const mapRowToLocalizedEntry = (row) => ({
  slug: row.slug,
  path: row.path,
  content: typeof row.data === 'string' ? JSON.parse(row.data) : (row.data || {}),
});

/**
 * Map multiple rows (one per locale) to a single Entry object.
 * Groups rows by base ID and reconstructs the locale map.
 * @param {Record<string, any>[]} rows Database rows for a single entry (one per locale).
 * @returns {Entry} Entry object.
 */
const mapRowsToEntry = (rows) => {
  if (rows.length === 0) {
    throw new Error('Cannot map empty rows to entry');
  }

  // Use the first row for common fields
  const firstRow = rows[0];

  /** @type {Record<string, LocalizedEntry>} */
  const locales = {};

  // Build the locales map from all rows
  for (const row of rows) {
    const locale = row.locale || '_default';
    const content = typeof row.data === 'string' ? JSON.parse(row.data) : (row.data || {});

    locales[locale] = {
      slug: row.slug,
      path: row.path,
      content,
    };
  }

  // Extract base ID (without locale suffix) for grouping
  const baseId = firstRow.id.replace(/_[a-z]{2}(-[A-Z]{2})?$/, '');

  /** @type {Entry} */
  const entry = {
    id: baseId,
    sha: firstRow.sha || '',
    slug: firstRow.slug,
    subPath: firstRow.path?.replace(/\.[^.]+$/, '') || firstRow.slug,
    locales,
  };

  // Add commit date if present
  if (firstRow.updated_at) {
    entry.commitDate = new Date(firstRow.updated_at);
  }

  // Add commit author if present
  if (firstRow.updated_by) {
    entry.commitAuthor = {
      name: firstRow.updated_by,
      email: '',
    };
  }

  return entry;
};

/**
 * Fetch all entries from the DuckDB table.
 * Groups rows by entry ID to reconstruct the full Entry objects.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional, creates new if not provided).
 * @returns {Promise<Entry[]>} Array of entries.
 */
export const fetchAllEntries = async (conn) => {
  const connection = conn || (await getConnection());

  debugLog('Fetching all entries...');

  try {
    const result = await connection.query('SELECT * FROM cms.entries ORDER BY id, locale');
    const rows = result.toArray();

    debugLog(`Fetched ${rows.length} entry rows`);

    // Group rows by entry ID
    /** @type {Map<string, Record<string, any>[]>} */
    const entryMap = new Map();

    for (const row of rows) {
      const id = row.id;

      if (!entryMap.has(id)) {
        entryMap.set(id, []);
      }

      entryMap.get(id)?.push(row);
    }

    // Convert grouped rows to Entry objects
    const entries = Array.from(entryMap.values()).map((rowGroup) => mapRowsToEntry(rowGroup));

    debugLog(`Mapped to ${entries.length} entries`);

    return entries;
  } catch (error) {
    throw new Error(
      `Failed to fetch all entries: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Fetch entries for a specific collection.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @param {string} collectionName Collection name to filter by.
 * @returns {Promise<Entry[]>} Array of entries in the collection.
 */
export const fetchEntriesByCollection = async (conn, collectionName) => {
  const connection = conn || (await getConnection());

  debugLog('Fetching entries for collection:', collectionName);

  try {
    const result = await connection.query(`
      SELECT * FROM cms.entries
      WHERE collection_name = ?
      ORDER BY id, locale
    `, collectionName);

    const rows = result.toArray();

    debugLog(`Fetched ${rows.length} entry rows for collection ${collectionName}`);

    // Group rows by entry ID
    /** @type {Map<string, Record<string, any>[]>} */
    const entryMap = new Map();

    for (const row of rows) {
      const id = row.id;

      if (!entryMap.has(id)) {
        entryMap.set(id, []);
      }

      entryMap.get(id)?.push(row);
    }

    // Convert grouped rows to Entry objects
    const entries = Array.from(entryMap.values()).map((rowGroup) => mapRowsToEntry(rowGroup));

    debugLog(`Mapped to ${entries.length} entries`);

    return entries;
  } catch (error) {
    throw new Error(
      `Failed to fetch entries by collection: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Fetch a single entry by ID.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @param {string} id Entry ID.
 * @returns {Promise<Entry | null>} Entry object or null if not found.
 */
export const fetchEntryById = async (conn, id) => {
  const connection = conn || (await getConnection());

  debugLog('Fetching entry by ID:', id);

  try {
    const result = await connection.query(`
      SELECT * FROM cms.entries
      WHERE id = ?
      ORDER BY locale
    `, id);

    const rows = result.toArray();

    if (rows.length === 0) {
      debugLog('Entry not found:', id);

      return null;
    }

    const entry = mapRowsToEntry(rows);

    debugLog('Entry fetched successfully:', id);

    return entry;
  } catch (error) {
    throw new Error(
      `Failed to fetch entry by ID: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Upsert (insert or update) an entry in the database.
 * Creates one row per locale for the entry using schema.js format.
 * Schema: id, collection, slug, locale, path, data, status, sha, created_at, updated_at, created_by, updated_by
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @param {Entry} entry Entry object to upsert.
 * @param {string} collectionName Collection name for the entry.
 * @returns {Promise<void>}
 */
export const upsertEntry = async (conn, entry, collectionName) => {
  const connection = conn || (await getConnection());

  debugLog('Upserting entry:', entry.id, 'to collection:', collectionName);

  try {
    // Ensure table exists
    await initEntriesTable(connection);

    // Prepare common values
    const { id, sha, slug, locales, commitAuthor } = entry;
    const updatedBy = commitAuthor?.name || null;

    // Insert one row per locale
    for (const [locale, localizedEntry] of Object.entries(locales)) {
      const { slug: localeSlug, path: localePath, content } = localizedEntry;
      const dataJson = JSON.stringify(content);

      // Create unique ID for this locale row
      const localeId = locale === '_default' ? id : `${id}_${locale}`;

      // Delete existing row for this locale (for clean upsert)
      await connection.query(`DELETE FROM cms.entries WHERE id = '${localeId}'`);

      // Insert using the schema.js format
      await connection.query(`
        INSERT INTO cms.entries (
          id, collection, slug, locale, path, data, status, sha, updated_at, updated_by
        ) VALUES (
          '${localeId}',
          '${collectionName}',
          '${localeSlug || slug}',
          '${locale}',
          '${localePath || `${collectionName}/${localeSlug || slug}`}',
          '${dataJson.replace(/'/g, "''")}',
          'published',
          '${sha || ''}',
          CURRENT_TIMESTAMP,
          ${updatedBy ? `'${updatedBy}'` : 'NULL'}
        )
      `);
    }

    debugLog('Entry upserted successfully:', entry.id);
  } catch (error) {
    debugLog('Error upserting entry:', error);
    throw new Error(
      `Failed to upsert entry: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Delete an entry by ID.
 * Removes all locale rows for the entry.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @param {string} id Entry ID to delete.
 * @returns {Promise<void>}
 */
export const deleteEntry = async (conn, id) => {
  const connection = conn || (await getConnection());

  debugLog('Deleting entry:', id);

  try {
    await connection.query('DELETE FROM cms.entries WHERE id = ?', id);

    debugLog('Entry deleted successfully:', id);
  } catch (error) {
    throw new Error(
      `Failed to delete entry: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Export all entries to a Parquet file buffer.
 * Uses DuckDB's native COPY TO for efficient Parquet serialization.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @returns {Promise<Uint8Array>} Parquet file bytes.
 */
export const exportEntriesToParquet = async (conn) => {
  const connection = conn || (await getConnection());

  debugLog('Exporting entries to Parquet...');

  try {
    // Export to in-memory virtual filesystem
    const tempPath = '/tmp/entries.parquet';

    await connection.query(`
      COPY (SELECT * FROM cms.entries ORDER BY id, locale)
      TO '${tempPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
    `);

    // Read the file from DuckDB's virtual filesystem
    // We need to get the database instance to access the filesystem
    const { initDuckDB } = await import('$lib/services/backends/duckdb/init');
    const db = await initDuckDB();
    const buffer = await db.copyFileToBuffer(tempPath);

    // Clean up temporary file
    await db.dropFile(tempPath);

    debugLog(`Exported ${buffer.length} bytes to Parquet`);

    return buffer;
  } catch (error) {
    throw new Error(
      `Failed to export entries to Parquet: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Get the count of entries in the database.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @returns {Promise<number>} Number of unique entries.
 */
export const getEntryCount = async (conn) => {
  const connection = conn || (await getConnection());

  try {
    const result = await connection.query('SELECT COUNT(DISTINCT id) as count FROM cms.entries');
    const rows = result.toArray();

    return rows[0]?.count || 0;
  } catch (error) {
    debugLog('Error getting entry count:', error);

    return 0;
  }
};

/**
 * Clear all entries from the database.
 * Useful for resetting state or testing.
 * @param {AsyncDuckDBConnection} [conn] Database connection (optional).
 * @returns {Promise<void>}
 */
export const clearAllEntries = async (conn) => {
  const connection = conn || (await getConnection());

  debugLog('Clearing all entries...');

  try {
    await connection.query('DELETE FROM cms.entries');

    debugLog('All entries cleared');
  } catch (error) {
    throw new Error(
      `Failed to clear entries: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};
