/**
 * DuckDB WASM initialization module.
 * Implements the worker thread pattern for UI responsiveness.
 * @module init
 */

import { get } from 'svelte/store';

import { REQUIRED_EXTENSIONS } from '$lib/services/backends/duckdb/constants';
import { prefs } from '$lib/services/user/prefs';

/**
 * @import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
 */

/**
 * Module-level singleton for the DuckDB database instance.
 * @type {AsyncDuckDB | null}
 */
let db = null;

/**
 * Module-level singleton for the active database connection.
 * @type {AsyncDuckDBConnection | null}
 */
let conn = null;

/**
 * Reference to the worker thread for cleanup.
 * @type {Worker | null}
 */
let worker = null;

/**
 * Track initialization state to prevent duplicate initialization.
 * @type {boolean}
 */
let isInitializing = false;

/**
 * Track if extensions have been loaded.
 * @type {boolean}
 */
let extensionsLoaded = false;

/**
 * Log debug information when developer mode is enabled.
 * @param {string} message Log message.
 * @param  {...any} args Additional arguments to log.
 */
const debugLog = (message, ...args) => {
  if (get(prefs).devModeEnabled) {
    // eslint-disable-next-line no-console
    console.info(`[DuckDB] ${message}`, ...args);
  }
};

/**
 * Create a worker from a cross-origin URL by fetching it and creating a Blob URL.
 * This is necessary because Web Workers have same-origin restrictions.
 * @param {string} workerUrl - URL of the worker script.
 * @returns {Promise<Worker>} A new Worker instance.
 * @throws {Error} When fetching or creating the worker fails.
 */
const createWorkerFromUrl = async (workerUrl) => {
  debugLog(`Fetching worker script from: ${workerUrl}`);

  try {
    const response = await fetch(workerUrl);

    if (!response.ok) {
      throw new Error(`Failed to fetch worker: ${response.status} ${response.statusText}`);
    }

    const workerScript = await response.text();
    const blob = new Blob([workerScript], { type: 'application/javascript' });
    const blobUrl = URL.createObjectURL(blob);

    debugLog('Created blob URL for worker');

    return new Worker(blobUrl);
  } catch (error) {
    throw new Error(
      `Failed to create worker from URL: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Initialize DuckDB WASM with worker thread pattern.
 * This function implements a singleton pattern to ensure only one DuckDB instance exists.
 * The worker thread pattern is REQUIRED for UI responsiveness during heavy operations.
 * @returns {Promise<AsyncDuckDB>} The initialized DuckDB instance.
 * @throws {Error} When DuckDB WASM initialization fails.
 */
export const initDuckDB = async () => {
  // Return existing instance if already initialized
  if (db !== null) {
    debugLog('Returning existing DuckDB instance');

    return db;
  }

  // Prevent duplicate initialization
  if (isInitializing) {
    debugLog('DuckDB initialization already in progress, waiting...');

    // Wait for initialization to complete
    while (isInitializing) {
      // eslint-disable-next-line no-await-in-loop, no-promise-executor-return
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    if (db !== null) {
      return db;
    }

    throw new Error('DuckDB initialization failed while waiting');
  }

  isInitializing = true;
  debugLog('Starting DuckDB WASM initialization...');

  try {
    // Dynamic import of @duckdb/duckdb-wasm to enable tree-shaking
    // and avoid loading the library until actually needed
    const duckdb = await import('@duckdb/duckdb-wasm');

    debugLog('Selecting optimal WASM bundle...');

    // Select the best bundle for the current browser
    // This automatically chooses between mvp (minimal viable product) and eh (exception handling)
    // based on browser capabilities
    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

    debugLog('Selected bundle:', bundle.mainModule);

    // Create worker thread using blob URL to avoid CORS issues
    // Web Workers have same-origin restrictions, so we fetch the script and create a blob
    worker = await createWorkerFromUrl(bundle.mainWorker);

    // Create logger for debugging
    const logger = new duckdb.ConsoleLogger(
      get(prefs).devModeEnabled ? duckdb.LogLevel.INFO : duckdb.LogLevel.WARNING,
    );

    // Create the async DuckDB instance with worker
    db = new duckdb.AsyncDuckDB(logger, worker);

    debugLog('Instantiating DuckDB WASM module...');

    // Instantiate the database with the selected WASM module
    // pthreadWorker enables multi-threading support if available
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    debugLog('DuckDB WASM initialized successfully');

    return db;
  } catch (error) {
    debugLog('DuckDB initialization failed:', error);

    // Clean up on failure
    if (worker) {
      worker.terminate();
      worker = null;
    }

    db = null;
    throw new Error(`Failed to initialize DuckDB WASM: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    isInitializing = false;
  }
};

/**
 * Load required extensions for lakehouse operations.
 * Extensions are loaded once and cached for the session.
 * @param {AsyncDuckDBConnection} connection Active database connection.
 * @returns {Promise<void>}
 * @throws {Error} When extension loading fails.
 */
const loadExtensions = async (connection) => {
  if (extensionsLoaded) {
    debugLog('Extensions already loaded');

    return;
  }

  debugLog('Loading required extensions:', REQUIRED_EXTENSIONS);

  for (const extension of REQUIRED_EXTENSIONS) {
    try {
      debugLog(`Installing extension: ${extension}`);
      // eslint-disable-next-line no-await-in-loop
      await connection.query(`INSTALL ${extension}`);
      // eslint-disable-next-line no-await-in-loop
      await connection.query(`LOAD ${extension}`);
      debugLog(`Extension ${extension} loaded successfully`);
    } catch (error) {
      throw new Error(
        `Failed to load DuckDB extension '${extension}': ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  extensionsLoaded = true;
  debugLog('All extensions loaded successfully');
};

/**
 * Configure DuckDB for S3-compatible storage with presigned URLs.
 * Sets reliable_head_requests=false because presigned URLs don't support HEAD requests.
 * @param {AsyncDuckDBConnection} connection Active database connection.
 * @returns {Promise<void>}
 */
const configureForPresignedURLs = async (connection) => {
  debugLog('Configuring DuckDB for presigned URL access...');

  // Presigned URLs are method-specific (GET only), HEAD requests fail with 403
  // This tells DuckDB to skip HEAD and use GET with Range headers instead
  await connection.query("SET reliable_head_requests = false");

  debugLog('Presigned URL configuration complete');
};

/**
 * Get an active database connection, creating one if necessary.
 * The connection is reused across operations for efficiency.
 * Extensions are automatically loaded on first connection.
 * @returns {Promise<AsyncDuckDBConnection>} The active database connection.
 * @throws {Error} When connection creation fails or DuckDB is not initialized.
 */
export const getConnection = async () => {
  // Ensure DuckDB is initialized
  if (db === null) {
    await initDuckDB();
  }

  // Return existing connection if available
  if (conn !== null) {
    debugLog('Returning existing connection');

    return conn;
  }

  if (db === null) {
    throw new Error('DuckDB is not initialized');
  }

  debugLog('Creating new database connection...');

  try {
    conn = await db.connect();

    // Load extensions on first connection
    await loadExtensions(conn);

    // Configure for presigned URL access
    await configureForPresignedURLs(conn);

    debugLog('Database connection established');

    return conn;
  } catch (error) {
    conn = null;
    throw new Error(
      `Failed to create DuckDB connection: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Close the DuckDB connection and terminate the worker.
 * This should be called during sign-out or when the backend is no longer needed.
 * @returns {Promise<void>}
 */
export const closeDuckDB = async () => {
  debugLog('Closing DuckDB...');

  // Close the connection if active
  if (conn !== null) {
    try {
      await conn.close();
      debugLog('Connection closed');
    } catch (error) {
      debugLog('Error closing connection:', error);
    }

    conn = null;
  }

  // Terminate the database
  if (db !== null) {
    try {
      await db.terminate();
      debugLog('Database terminated');
    } catch (error) {
      debugLog('Error terminating database:', error);
    }

    db = null;
  }

  // Terminate the worker
  if (worker !== null) {
    try {
      worker.terminate();
      debugLog('Worker terminated');
    } catch (error) {
      debugLog('Error terminating worker:', error);
    }

    worker = null;
  }

  // Reset extension state
  extensionsLoaded = false;

  debugLog('DuckDB closed successfully');
};

/**
 * Check if DuckDB is currently initialized and ready.
 * @returns {boolean} True if DuckDB is initialized.
 */
export const isDuckDBReady = () => db !== null && !isInitializing;

/**
 * Check if a connection is currently active.
 * @returns {boolean} True if a connection is active.
 */
export const hasActiveConnection = () => conn !== null;
