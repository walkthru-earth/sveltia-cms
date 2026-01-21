/**
 * @fileoverview DuckDB schema definitions for Sveltia CMS entries and assets.
 * Provides SQL table schemas and initialization functions for the content management system.
 */

/**
 * Get the SQL statement to create the entries table.
 * Stores content entries with localization, versioning, and metadata support.
 *
 * @returns {string} SQL CREATE TABLE statement for entries.
 */
export function getEntriesTableSQL() {
  return `
    CREATE TABLE IF NOT EXISTS cms.entries (
      id VARCHAR PRIMARY KEY,
      collection VARCHAR NOT NULL,
      slug VARCHAR NOT NULL,
      locale VARCHAR DEFAULT 'en',
      path VARCHAR NOT NULL,
      data JSON NOT NULL,
      status VARCHAR DEFAULT 'draft',
      sha VARCHAR,
      created_at TIMESTAMP DEFAULT current_timestamp,
      updated_at TIMESTAMP DEFAULT current_timestamp,
      created_by VARCHAR,
      updated_by VARCHAR,
      UNIQUE (collection, slug, locale)
    )
  `.trim();
}

/**
 * Get the SQL statement to create the assets table.
 * Stores media assets with metadata, dimensions, and content/URL references.
 *
 * @returns {string} SQL CREATE TABLE statement for assets.
 */
export function getAssetsTableSQL() {
  return `
    CREATE TABLE IF NOT EXISTS cms.assets (
      id VARCHAR PRIMARY KEY,
      path VARCHAR NOT NULL UNIQUE,
      filename VARCHAR NOT NULL,
      folder VARCHAR,
      mime_type VARCHAR,
      size BIGINT,
      width INTEGER,
      height INTEGER,
      content BLOB,
      storage_url VARCHAR,
      metadata JSON,
      sha VARCHAR,
      created_at TIMESTAMP DEFAULT current_timestamp,
      updated_at TIMESTAMP DEFAULT current_timestamp
    )
  `.trim();
}

/**
 * Initialize the complete database schema.
 * Creates the cms schema and all required tables for Sveltia CMS.
 *
 * @param {import('@duckdb/duckdb-wasm').AsyncDuckDBConnection} conn - Active database connection.
 * @returns {Promise<void>}
 * @throws {Error} If schema creation fails.
 */
export async function initializeSchema(conn) {
  try {
    // Create the cms schema namespace
    await conn.query('CREATE SCHEMA IF NOT EXISTS cms');

    // Create entries table for content
    await conn.query(getEntriesTableSQL());

    // Create assets table for media files
    await conn.query(getAssetsTableSQL());
  } catch (error) {
    throw new Error(`Failed to initialize database schema: ${error.message}`);
  }
}

/**
 * Check if the cms schema and required tables exist.
 * Verifies that the database has been properly initialized.
 *
 * @param {import('@duckdb/duckdb-wasm').AsyncDuckDBConnection} conn - Active database connection.
 * @returns {Promise<boolean>} True if schema and tables exist, false otherwise.
 */
export async function schemaExists(conn) {
  try {
    // Check if cms schema exists
    const schemaResult = await conn.query(`
      SELECT schema_name
      FROM information_schema.schemata
      WHERE schema_name = 'cms'
    `);
    const schemaRows = schemaResult.toArray();

    if (schemaRows.length === 0) {
      return false;
    }

    // Check if both required tables exist
    const tablesResult = await conn.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'cms'
        AND table_name IN ('entries', 'assets')
    `);
    const tableRows = tablesResult.toArray();

    // Both tables must exist
    return tableRows.length === 2;
  } catch (error) {
    // If information_schema queries fail, schema doesn't exist
    return false;
  }
}

/**
 * Get the SQL to create indices for optimized queries.
 * These indices improve performance for common query patterns in Sveltia CMS.
 *
 * @returns {string[]} Array of SQL CREATE INDEX statements.
 */
export function getIndicesSQL() {
  return [
    // Index for querying entries by collection
    `CREATE INDEX IF NOT EXISTS idx_entries_collection
     ON cms.entries(collection)`,

    // Index for querying entries by status
    `CREATE INDEX IF NOT EXISTS idx_entries_status
     ON cms.entries(status)`,

    // Index for querying entries by collection and locale
    `CREATE INDEX IF NOT EXISTS idx_entries_collection_locale
     ON cms.entries(collection, locale)`,

    // Index for querying assets by folder
    `CREATE INDEX IF NOT EXISTS idx_assets_folder
     ON cms.assets(folder)`,

    // Index for querying assets by mime type
    `CREATE INDEX IF NOT EXISTS idx_assets_mime_type
     ON cms.assets(mime_type)`,

    // Index for temporal queries on entries
    `CREATE INDEX IF NOT EXISTS idx_entries_updated_at
     ON cms.entries(updated_at DESC)`,

    // Index for temporal queries on assets
    `CREATE INDEX IF NOT EXISTS idx_assets_updated_at
     ON cms.assets(updated_at DESC)`,
  ];
}

/**
 * Create performance indices on schema tables.
 * Should be called after schema initialization for optimized query performance.
 *
 * @param {import('@duckdb/duckdb-wasm').AsyncDuckDBConnection} conn - Active database connection.
 * @returns {Promise<void>}
 */
export async function createIndices(conn) {
  const indices = getIndicesSQL();

  try {
    for (const indexSQL of indices) {
      await conn.query(indexSQL);
    }
  } catch (error) {
    throw new Error(`Failed to create indices: ${error.message}`);
  }
}

/**
 * Drop all tables and schema (for testing or reset).
 * WARNING: This will delete all data.
 *
 * @param {import('@duckdb/duckdb-wasm').AsyncDuckDBConnection} conn - Active database connection.
 * @returns {Promise<void>}
 */
export async function dropSchema(conn) {
  try {
    await conn.query('DROP SCHEMA IF EXISTS cms CASCADE');
  } catch (error) {
    throw new Error(`Failed to drop schema: ${error.message}`);
  }
}

/**
 * Get table statistics for monitoring and debugging.
 *
 * @param {import('@duckdb/duckdb-wasm').AsyncDuckDBConnection} conn - Active database connection.
 * @returns {Promise<{entries: number, assets: number}>} Row counts for each table.
 */
export async function getTableStats(conn) {
  try {
    const entriesResult = await conn.query('SELECT COUNT(*) as count FROM cms.entries');
    const assetsResult = await conn.query('SELECT COUNT(*) as count FROM cms.assets');

    const entriesCount = entriesResult.toArray()[0]?.count ?? 0;
    const assetsCount = assetsResult.toArray()[0]?.count ?? 0;

    return {
      entries: Number(entriesCount),
      assets: Number(assetsCount),
    };
  } catch (error) {
    throw new Error(`Failed to get table statistics: ${error.message}`);
  }
}
