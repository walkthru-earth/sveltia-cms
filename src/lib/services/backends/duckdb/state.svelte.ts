/**
 * DuckDB Backend Reactive State Management
 * Uses Svelte 5 runes for reactive state tracking
 *
 * This module provides a centralized state management solution for the DuckDB backend,
 * tracking connection status, loading states, authentication tokens, and errors.
 */

/**
 * State interface for internal reactive state object
 * Wrapping primitives in an object ensures proper reactivity when exported
 */
interface DuckDBStateData {
  /** Whether the DuckDB connection is established */
  connected: boolean;
  /** Whether a loading operation is in progress */
  loading: boolean;
  /** Whether the DuckDB instance is being initialized */
  initializing: boolean;
  /** Current error state, if any */
  error: Error | null;
  /** Authentication session token for cloud storage */
  sessionToken: string | null;
  /** Unix timestamp when the current credentials expire */
  credentialExpiry: number;
}

/**
 * DuckDB Backend State Manager
 *
 * Manages reactive state for the DuckDB backend using Svelte 5 runes.
 * This class provides a reactive interface for tracking connection status,
 * loading states, authentication tokens, and errors.
 *
 * @example
 * ```typescript
 * import { duckdbState } from './state.svelte';
 *
 * // Check connection status
 * if (duckdbState.connected) {
 *   console.log('DuckDB is connected');
 * }
 *
 * // Update state
 * duckdbState.setConnected(true);
 * duckdbState.setSessionToken('token-123');
 *
 * // Check derived state
 * if (duckdbState.isReady) {
 *   // Ready to perform operations
 * }
 * ```
 */
export class DuckDBState {
  /**
   * Internal reactive state object
   * IMPORTANT: Primitives are wrapped in an object to maintain reactivity when exported
   */
  private state = $state<DuckDBStateData>({
    connected: false,
    loading: false,
    initializing: false,
    error: null,
    sessionToken: null,
    credentialExpiry: 0,
  });

  // ============================================================================
  // Getters - Provide reactive access to state properties
  // ============================================================================

  /** Whether the DuckDB connection is established */
  get connected(): boolean {
    return this.state.connected;
  }

  /** Whether a loading operation is in progress */
  get loading(): boolean {
    return this.state.loading;
  }

  /** Whether the DuckDB instance is being initialized */
  get initializing(): boolean {
    return this.state.initializing;
  }

  /** Current error state, if any */
  get error(): Error | null {
    return this.state.error;
  }

  /** Authentication session token for cloud storage */
  get sessionToken(): string | null {
    return this.state.sessionToken;
  }

  /** Unix timestamp when the current credentials expire */
  get credentialExpiry(): number {
    return this.state.credentialExpiry;
  }

  // ============================================================================
  // Derived State - Computed values using $derived
  // ============================================================================

  /** Whether the backend is ready for operations (connected and not loading) */
  get isReady(): boolean {
    return $derived(this.state.connected && !this.state.loading);
  }

  /** Whether an error is currently present */
  get hasError(): boolean {
    return $derived(this.state.error !== null);
  }

  /** Whether the current credentials have expired */
  get isCredentialExpired(): boolean {
    return $derived(Date.now() > this.state.credentialExpiry);
  }

  // ============================================================================
  // Setters - Update state properties
  // ============================================================================

  /**
   * Set the connection status
   * @param value - True if connected, false otherwise
   */
  setConnected(value: boolean): void {
    this.state.connected = value;
  }

  /**
   * Set the loading status
   * @param value - True if loading, false otherwise
   */
  setLoading(value: boolean): void {
    this.state.loading = value;
  }

  /**
   * Set the initializing status
   * @param value - True if initializing, false otherwise
   */
  setInitializing(value: boolean): void {
    this.state.initializing = value;
  }

  /**
   * Set the error state
   * @param error - Error object or null to clear
   */
  setError(error: Error | null): void {
    this.state.error = error;
  }

  /**
   * Set the authentication session token
   * @param token - Session token string or null to clear
   */
  setSessionToken(token: string | null): void {
    this.state.sessionToken = token;
  }

  /**
   * Set the credential expiry timestamp
   * @param expiry - Unix timestamp when credentials expire
   */
  setCredentialExpiry(expiry: number): void {
    this.state.credentialExpiry = expiry;
  }

  // ============================================================================
  // Utility Methods
  // ============================================================================

  /**
   * Reset all state to initial values
   * Useful for cleanup or when switching backends
   */
  reset(): void {
    this.state.connected = false;
    this.state.loading = false;
    this.state.initializing = false;
    this.state.error = null;
    this.state.sessionToken = null;
    this.state.credentialExpiry = 0;
  }

  /**
   * Get a snapshot of the current state
   * @returns Immutable copy of current state
   */
  getSnapshot(): Readonly<DuckDBStateData> {
    return {
      connected: this.state.connected,
      loading: this.state.loading,
      initializing: this.state.initializing,
      error: this.state.error,
      sessionToken: this.state.sessionToken,
      credentialExpiry: this.state.credentialExpiry,
    };
  }
}

/**
 * Singleton instance of DuckDB state manager
 * Import this to access and modify the global DuckDB backend state
 *
 * @example
 * ```typescript
 * import { duckdbState } from './state.svelte';
 *
 * // Use in components or modules
 * duckdbState.setConnected(true);
 * console.log(duckdbState.isReady);
 * ```
 */
export const duckdbState = new DuckDBState();
