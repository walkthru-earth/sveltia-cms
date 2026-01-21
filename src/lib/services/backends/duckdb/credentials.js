/**
 * DuckDB backend credentials management.
 * Handles presigned URL fetching, caching, and credential proxy communication.
 * @module credentials
 */

import { DEFAULT_PRESIGN_EXPIRY } from './constants.js';

/**
 * @typedef {object} CredentialProxyConfig
 * @property {string} proxyUrl - Credential proxy URL.
 * @property {string} provider - Storage provider name ('s3' | 'r2' | 'gcs' | 'azure').
 */

/**
 * @typedef {object} PresignedUrlCache
 * @property {Record<string, string>} urls - Cached URLs keyed by path.
 * @property {number} expiry - Expiry timestamp in milliseconds.
 */

/**
 * @typedef {object} PresignResponse
 * @property {string} url - The presigned URL.
 * @property {number} expiresIn - Expiry time in seconds.
 * @property {string} path - The file path.
 * @property {string} operation - The operation type ('GET' | 'PUT' | 'DELETE').
 */

/**
 * @typedef {object} PresignBatchResponse
 * @property {Record<string, string>} urls - Map of paths to presigned URLs.
 * @property {number} expiresIn - Expiry time in seconds.
 */

/**
 * Credential proxy configuration.
 * @type {{ proxyUrl: string | null; provider: string | null }}
 */
const config = {
  proxyUrl: null,
  provider: null,
};

/**
 * Session token for authenticating with the credential proxy.
 * @type {string | null}
 */
let sessionToken = null;

/**
 * Cache for presigned GET URLs.
 * @type {PresignedUrlCache}
 */
const cache = {
  urls: {},
  expiry: 0,
};

/**
 * Buffer time in milliseconds before expiry to refresh URLs.
 * Refresh URLs 1 minute (60 seconds) before they expire.
 * @type {number}
 */
const REFRESH_BUFFER_MS = 60 * 1000;

/**
 * Configure the credential proxy settings.
 * @param {CredentialProxyConfig} proxyConfig - Proxy configuration.
 * @throws {Error} If proxyUrl or provider is missing.
 */
export function configureProxy(proxyConfig) {
  if (!proxyConfig.proxyUrl) {
    throw new Error('Credential proxy URL is required');
  }

  if (!proxyConfig.provider) {
    throw new Error('Storage provider is required');
  }

  config.proxyUrl = proxyConfig.proxyUrl;
  config.provider = proxyConfig.provider;
}

/**
 * Set the session token for API authentication.
 * @param {string} token - JWT session token.
 * @throws {Error} If token is empty or invalid.
 */
export function setSessionToken(token) {
  if (!token || typeof token !== 'string') {
    throw new Error('Valid session token is required');
  }

  sessionToken = token;
}

/**
 * Clear all cached credentials and session token.
 * Called on sign out or authentication failure.
 */
export function clearCredentials() {
  sessionToken = null;
  cache.urls = {};
  cache.expiry = 0;
}

/**
 * Check if a cached URL is still valid.
 * @returns {boolean} True if cache is valid and not expired.
 */
function isCacheValid() {
  const now = Date.now();
  return cache.expiry > now + REFRESH_BUFFER_MS;
}

/**
 * Update the cache with new URLs and expiry time.
 * @param {Record<string, string>} urls - Map of paths to presigned URLs.
 * @param {number} expiresIn - Expiry time in seconds.
 */
function updateCache(urls, expiresIn) {
  const now = Date.now();
  cache.urls = { ...cache.urls, ...urls };
  cache.expiry = now + expiresIn * 1000;
}

/**
 * Make an authenticated request to the credential proxy.
 * @param {string} endpoint - API endpoint path.
 * @param {object} body - Request body.
 * @returns {Promise<any>} Response data.
 * @throws {Error} If request fails or authentication is invalid.
 */
async function proxyRequest(endpoint, body) {
  if (!config.proxyUrl) {
    throw new Error('Credential proxy not configured. Call configureProxy() first.');
  }

  if (!sessionToken) {
    throw new Error('Not authenticated. Call setSessionToken() first.');
  }

  const url = `${config.proxyUrl}${endpoint}`;

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${sessionToken}`,
      },
      body: JSON.stringify({ provider: config.provider, ...body }),
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');

      if (response.status === 401 || response.status === 403) {
        // Clear credentials on auth failure
        clearCredentials();
        throw new Error(`Authentication failed: ${errorText}`);
      }

      throw new Error(`Credential proxy request failed (${response.status}): ${errorText}`);
    }

    return await response.json();
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }

    throw new Error(`Failed to communicate with credential proxy: ${String(error)}`);
  }
}

/**
 * Get a presigned URL for a file operation.
 * GET URLs are cached until expiry. PUT and DELETE URLs are never cached.
 * @param {string} path - File path in cloud storage.
 * @param {'GET' | 'PUT' | 'DELETE'} [operation='GET'] - Operation type.
 * @param {string} [contentType='application/octet-stream'] - Content type for PUT operations.
 * @returns {Promise<string>} Presigned URL.
 * @throws {Error} If path is invalid or request fails.
 */
export async function getPresignedUrl(
  path,
  operation = 'GET',
  contentType = 'application/octet-stream',
) {
  if (!path || typeof path !== 'string') {
    throw new Error('Valid file path is required');
  }

  // Check cache for GET operations
  if (operation === 'GET' && isCacheValid() && cache.urls[path]) {
    return cache.urls[path];
  }

  // Request new presigned URL
  /** @type {PresignResponse} */
  const response = await proxyRequest('/presign', {
    operation,
    path,
    ...(operation === 'PUT' && { contentType }),
  });

  // Cache GET URLs only
  if (operation === 'GET') {
    updateCache({ [path]: response.url }, response.expiresIn);
  }

  return response.url;
}

/**
 * Fetch presigned URLs for multiple paths in batch.
 * Only supports GET operations. Results are cached.
 * @param {string[]} paths - Array of file paths.
 * @returns {Promise<Record<string, string>>} Map of paths to presigned URLs.
 * @throws {Error} If paths are invalid or request fails.
 */
export async function fetchPresignedUrls(paths) {
  if (!Array.isArray(paths) || paths.length === 0) {
    throw new Error('Valid array of paths is required');
  }

  // Filter out empty paths
  const validPaths = paths.filter((p) => p && typeof p === 'string');

  if (validPaths.length === 0) {
    throw new Error('No valid paths provided');
  }

  // Check which paths need to be fetched
  const pathsToFetch = isCacheValid()
    ? validPaths.filter((p) => !cache.urls[p])
    : validPaths;

  // If all paths are cached, return from cache
  if (pathsToFetch.length === 0) {
    return Object.fromEntries(validPaths.map((p) => [p, cache.urls[p]]));
  }

  // Fetch missing URLs
  /** @type {PresignBatchResponse} */
  const response = await proxyRequest('/presign-batch', {
    paths: pathsToFetch,
    operation: 'GET',
  });

  // Update cache with new URLs
  updateCache(response.urls, response.expiresIn);

  // Return all requested URLs (from cache and newly fetched)
  return Object.fromEntries(validPaths.map((p) => [p, cache.urls[p]]));
}

/**
 * Get the current cache state for debugging.
 * @returns {{ size: number; expiry: number; expiresIn: number }} Cache state.
 */
export function getCacheState() {
  const now = Date.now();
  return {
    size: Object.keys(cache.urls).length,
    expiry: cache.expiry,
    expiresIn: Math.max(0, Math.floor((cache.expiry - now) / 1000)),
  };
}

/**
 * Exchange OAuth token for a session token with the credential proxy.
 * This must be called after OAuth authentication to get cloud storage access.
 * @param {object} args - Arguments.
 * @param {string} args.oauthToken - OAuth access token from GitHub/GitLab.
 * @param {string} args.provider - OAuth provider ('github' or 'gitlab').
 * @param {string} args.proxyURL - Credential proxy URL.
 * @returns {Promise<{ sessionToken: string, user: object, expiresIn: number }>}
 * Session token, user info, and expiry time.
 * @throws {Error} When token exchange fails.
 */
export async function exchangeToken({ oauthToken, provider, proxyURL }) {
  if (!oauthToken || typeof oauthToken !== 'string') {
    throw new Error('Valid OAuth token is required');
  }

  if (!provider || (provider !== 'github' && provider !== 'gitlab')) {
    throw new Error('Provider must be either "github" or "gitlab"');
  }

  if (!proxyURL || typeof proxyURL !== 'string') {
    throw new Error('Valid credential proxy URL is required');
  }

  const url = `${proxyURL}/token-exchange`;

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        token: oauthToken,
        provider,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');

      throw new Error(
        `Token exchange failed (${response.status}): ${errorText}`,
      );
    }

    const data = await response.json();

    if (!data.sessionToken || !data.user) {
      throw new Error('Invalid response from credential proxy: missing sessionToken or user');
    }

    return {
      sessionToken: data.sessionToken,
      user: data.user,
      expiresIn: data.expiresIn || DEFAULT_PRESIGN_EXPIRY,
    };
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }

    throw new Error(`Failed to exchange OAuth token: ${String(error)}`);
  }
}
