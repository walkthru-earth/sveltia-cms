/**
 * DuckDB backend authentication module.
 * Handles OAuth-based authentication via GitHub/GitLab and credential proxy token exchange.
 * @module auth
 */

import { get } from 'svelte/store';

import { BACKEND_NAME } from '$lib/services/backends/duckdb/constants';
import {
  clearCredentials,
  configureProxy,
  exchangeToken,
  isSessionExpiringSoon,
  isSessionValid,
  setSessionToken,
} from '$lib/services/backends/duckdb/credentials';
import { closeDuckDB, initDuckDB } from '$lib/services/backends/duckdb/init';
import { repository, resetRepository } from '$lib/services/backends/duckdb/repository';
import { authorize, openPopup } from '$lib/services/backends/git/shared/auth';
import { cmsConfig } from '$lib/services/config';
import { prefs } from '$lib/services/user/prefs';

/**
 * @import { SignInOptions, User } from '$lib/types/private';
 */

/**
 * API configuration for GitHub OAuth.
 * This is used when the DuckDB backend is configured to use GitHub as the OAuth provider.
 * @type {import('$lib/types/private').ApiEndpointConfig}
 */
const GITHUB_API_CONFIG = {
  clientId: '',
  authScope: 'repo',
  authURL: 'https://github.com/login/oauth/authorize',
  tokenURL: 'https://github.com/login/oauth/access_token',
  authScheme: 'token',
  restBaseURL: 'https://api.github.com',
  graphqlBaseURL: 'https://api.github.com/graphql',
};

/**
 * API configuration for GitLab OAuth.
 * This is used when the DuckDB backend is configured to use GitLab as the OAuth provider.
 * @type {import('$lib/types/private').ApiEndpointConfig}
 */
const GITLAB_API_CONFIG = {
  clientId: '',
  authScope: 'api',
  authURL: 'https://gitlab.com/oauth/authorize',
  tokenURL: 'https://gitlab.com/oauth/token',
  authScheme: 'Bearer',
  restBaseURL: 'https://gitlab.com/api/v4',
  graphqlBaseURL: 'https://gitlab.com/api/graphql',
};

/**
 * Log debug information when developer mode is enabled.
 * @param {string} message Log message.
 * @param  {...any} args Additional arguments to log.
 */
const debugLog = (message, ...args) => {
  if (get(prefs).devModeEnabled) {
    // eslint-disable-next-line no-console
    console.info(`[DuckDB Auth] ${message}`, ...args);
  }
};

/**
 * Get the OAuth provider from the CMS configuration.
 * Defaults to 'github' if not specified.
 * @returns {'github' | 'gitlab'} OAuth provider.
 */
const getOAuthProvider = () => {
  const { backend } = get(cmsConfig) ?? {};
  const provider = backend?.oauth_provider || 'github';

  if (provider !== 'github' && provider !== 'gitlab') {
    debugLog(`Invalid OAuth provider: ${provider}, defaulting to github`);

    return 'github';
  }

  return provider;
};

/**
 * Get the API configuration for the specified OAuth provider.
 * Uses the credential proxy for OAuth flow instead of direct provider auth.
 * @param {'github' | 'gitlab'} provider OAuth provider.
 * @returns {import('$lib/types/private').ApiEndpointConfig} API configuration.
 */
const getApiConfig = (provider) => {
  const { backend } = get(cmsConfig) ?? {};
  const baseConfig = provider === 'gitlab' ? GITLAB_API_CONFIG : GITHUB_API_CONFIG;

  // Use credential proxy for OAuth - the proxy handles the actual OAuth flow
  const credentialProxy = backend?.credential_proxy;

  // Override with credential proxy as auth URL
  return {
    ...baseConfig,
    clientId: backend?.oauth_client_id || baseConfig.clientId,
    // Use credential proxy's /auth endpoint for OAuth initiation
    authURL: credentialProxy ? `${credentialProxy}/auth` : baseConfig.authURL,
    tokenURL: backend?.oauth_token_url || baseConfig.tokenURL,
  };
};

/**
 * Perform OAuth authentication via the credential proxy.
 * This custom implementation sends the OAuth provider name (github/gitlab)
 * instead of the backend name (duckdb) to the credential proxy.
 * @param {string} credentialProxy Credential proxy URL.
 * @param {'github' | 'gitlab'} oauthProvider OAuth provider name.
 * @returns {Promise<{ token: string } | undefined>} OAuth token or undefined if aborted.
 */
const performOAuth = async (credentialProxy, oauthProvider) => {
  const { hostname } = window.location;
  // Use current hostname as site_id, or 'localhost' for local development
  const siteId = hostname === 'localhost' ? 'localhost' : hostname;

  const params = new URLSearchParams({
    provider: oauthProvider, // Send 'github' or 'gitlab', not 'duckdb'
    site_id: siteId,
  });

  const authURL = `${credentialProxy}/auth?${params}`;

  debugLog(`Opening OAuth popup: ${authURL}`);

  // Use the shared authorize function which handles popup and postMessage
  return authorize({
    backendName: oauthProvider, // Use OAuth provider name for postMessage matching
    authURL,
  });
};

/**
 * Sign in with the DuckDB backend.
 * Authenticates via OAuth (GitHub/GitLab), exchanges token with credential proxy,
 * and initializes DuckDB WASM.
 * @param {SignInOptions} options Sign-in options.
 * @returns {Promise<User | void>} User info, or nothing when sign-in cannot proceed.
 * @throws {Error} When there was an authentication error.
 */
export const signIn = async (options) => {
  debugLog('Starting sign-in flow...');

  try {
    // Get credential proxy URL from repository config
    const { credentialProxy } = repository;

    if (!credentialProxy) {
      throw new Error('Credential proxy URL not configured');
    }

    // Determine which OAuth provider to use
    const oauthProvider = getOAuthProvider();

    debugLog(`Using OAuth provider: ${oauthProvider}`);

    // Check if we already have a token passed in
    if (options?.token) {
      debugLog('Using existing token from options');

      // Store session token
      setSessionToken(options.token);

      // Configure credential proxy
      const { storageProvider } = repository;

      configureProxy({
        proxyUrl: credentialProxy,
        provider: storageProvider || 's3',
      });

      // Initialize DuckDB WASM
      debugLog('Initializing DuckDB WASM...');
      await initDuckDB();
      debugLog('DuckDB WASM initialized successfully');

      return {
        backendName: BACKEND_NAME,
        token: options.token,
      };
    }

    // Perform OAuth authentication via credential proxy
    const result = await performOAuth(credentialProxy, oauthProvider);

    if (!result?.token) {
      debugLog('OAuth authentication aborted or failed');

      return undefined;
    }

    const { token: oauthToken } = result;

    debugLog('OAuth authentication successful, exchanging token...');

    // Exchange OAuth token for session token with credential proxy
    const { sessionToken, user, expiresIn } = await exchangeToken({
      oauthToken,
      provider: oauthProvider,
      proxyURL: credentialProxy,
    });

    debugLog('Token exchange successful, session expires in:', expiresIn);

    // Store session token with expiry tracking
    setSessionToken(sessionToken, expiresIn);

    // Configure credential proxy for presigned URL requests
    const { storageProvider } = repository;

    configureProxy({
      proxyUrl: credentialProxy,
      provider: storageProvider || 's3',
    });

    // Initialize DuckDB WASM in a worker thread
    debugLog('Initializing DuckDB WASM...');
    await initDuckDB();
    debugLog('DuckDB WASM initialized successfully');

    // Enhance user object with backend information
    /** @type {User} */
    const enhancedUser = {
      ...user,
      backendName: BACKEND_NAME,
      token: sessionToken,
    };

    debugLog('Sign-in completed successfully');

    return enhancedUser;
  } catch (error) {
    debugLog('Sign-in failed:', error);

    // Clean up on failure
    clearCredentials();

    throw new Error(
      `Failed to sign in: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Sign out from the DuckDB backend.
 * Closes DuckDB connection, clears credentials, and resets repository state.
 * @returns {Promise<void>}
 */
export const signOut = async () => {
  debugLog('Starting sign-out...');

  try {
    // Close DuckDB connection and terminate worker
    await closeDuckDB();
    debugLog('DuckDB connection closed');

    // Clear credentials and presigned URL cache
    clearCredentials();
    debugLog('Credentials cleared');

    // Reset repository information
    resetRepository();
    debugLog('Repository state reset');

    debugLog('Sign-out completed successfully');
  } catch (error) {
    debugLog('Error during sign-out:', error);

    // Even if there's an error, try to clean up as much as possible
    try {
      clearCredentials();
      resetRepository();
    } catch {
      // Ignore cleanup errors
    }

    throw new Error(
      `Error during sign-out: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Refresh the session by re-authenticating via OAuth.
 * This is called when the session is expired or expiring soon.
 * Unlike signIn, this doesn't reinitialize DuckDB (already running).
 * @returns {Promise<boolean>} True if refresh succeeded, false if user cancelled.
 * @throws {Error} When refresh fails due to an error (not user cancellation).
 */
export const refreshSession = async () => {
  debugLog('Starting session refresh...');

  try {
    const { credentialProxy, storageProvider } = repository;

    if (!credentialProxy) {
      throw new Error('Credential proxy URL not configured');
    }

    const oauthProvider = getOAuthProvider();

    debugLog(`Refreshing session via ${oauthProvider} OAuth...`);

    // Perform OAuth authentication
    const result = await performOAuth(credentialProxy, oauthProvider);

    if (!result?.token) {
      debugLog('Session refresh cancelled by user');

      return false;
    }

    const { token: oauthToken } = result;

    // Exchange for new session token
    const { sessionToken, expiresIn } = await exchangeToken({
      oauthToken,
      provider: oauthProvider,
      proxyURL: credentialProxy,
    });

    // Update session token with new expiry
    setSessionToken(sessionToken, expiresIn);

    // Reconfigure proxy (in case it was cleared)
    configureProxy({
      proxyUrl: credentialProxy,
      provider: storageProvider || 's3',
    });

    debugLog('Session refreshed successfully, expires in:', expiresIn);

    return true;
  } catch (error) {
    debugLog('Session refresh failed:', error);

    throw new Error(
      `Failed to refresh session: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

/**
 * Check if session needs refresh and refresh if necessary.
 * Returns true if session is valid (either was valid or successfully refreshed).
 * @returns {Promise<boolean>} True if session is valid after check.
 */
export const ensureValidSession = async () => {
  // Session is valid and not expiring soon
  if (isSessionValid() && !isSessionExpiringSoon()) {
    return true;
  }

  debugLog('Session expired or expiring soon, attempting refresh...');

  // Try to refresh
  return refreshSession();
};
