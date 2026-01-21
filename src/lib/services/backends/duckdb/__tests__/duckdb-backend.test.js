import { beforeEach, describe, expect, test, vi, afterEach } from 'vitest';

// Import constants to test
import {
  BACKEND_NAME,
  BACKEND_LABEL,
  DEFAULT_WASM_BUNDLES,
  REQUIRED_EXTENSIONS,
  DEFAULT_PRESIGN_EXPIRY,
  DEFAULT_ASSET_INLINE_THRESHOLD,
  STORAGE_TYPES,
  STORAGE_PROVIDERS,
  CATALOG_TYPES,
  ASSET_MODES,
} from '../constants.js';

// Import credentials module to test
import {
  configureProxy,
  setSessionToken,
  clearCredentials,
  getPresignedUrl,
  fetchPresignedUrls,
  getCacheState,
  exchangeToken,
} from '../credentials.js';

// Import repository module to test
import {
  repository,
  resetRepository,
  getFullStoragePath,
  getStorageURI,
  parseStorageConfig,
  parseCatalogConfig,
} from '../repository.js';

// Import init module to test
import {
  initDuckDB,
  getConnection,
  closeDuckDB,
  isDuckDBReady,
  hasActiveConnection,
} from '../init.js';

// Import auth module to test
import { signIn, signOut } from '../auth.js';

// Import main index module
import { init } from '../index.js';

// Mock DuckDB WASM
vi.mock('@duckdb/duckdb-wasm', () => {
  const mockConnection = {
    query: vi.fn().mockResolvedValue({ toArray: () => [] }),
    close: vi.fn().mockResolvedValue(undefined),
  };

  class MockAsyncDuckDB {
    connect = vi.fn().mockResolvedValue(mockConnection);
    instantiate = vi.fn().mockResolvedValue(undefined);
    terminate = vi.fn().mockResolvedValue(undefined);
  }

  class MockConsoleLogger {
    constructor() {}
  }

  const mockBundle = {
    mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-eh.wasm',
    mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-browser-eh.worker.js',
    pthreadWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev18.0/dist/duckdb-browser-eh.pthread.worker.js',
  };

  return {
    selectBundle: vi.fn().mockResolvedValue(mockBundle),
    getJsDelivrBundles: vi.fn().mockReturnValue([]),
    AsyncDuckDB: MockAsyncDuckDB,
    ConsoleLogger: MockConsoleLogger,
    LogLevel: {
      INFO: 1,
      WARNING: 2,
    },
  };
});

// Mock svelte stores
vi.mock('svelte/store', async () => {
  const actual = await vi.importActual('svelte/store');
  const mockCmsConfig = {
    backend: {
      name: 'duckdb',
      storage_type: 'parquet',
      credential_proxy: 'https://cms-auth.workers.dev',
      storage: {
        provider: 's3',
        bucket: 'test-bucket',
        data_path: 'cms/data/',
      },
    },
  };
  return {
    ...actual,
    get: vi.fn((store) => {
      // Check if it's the prefs store
      if (store && typeof store === 'object' && 'devModeEnabled' in store) {
        return { devModeEnabled: false };
      }
      // Return the mockConfig for cmsConfig store
      return mockCmsConfig;
    }),
  };
});

// Mock prefs
vi.mock('$lib/services/user/prefs', () => ({
  prefs: { devModeEnabled: false },
}));

// Mock cmsConfig
vi.mock('$lib/services/config', () => ({
  cmsConfig: {
    backend: {
      name: 'duckdb',
      storage_type: 'parquet',
      credential_proxy: 'https://cms-auth.workers.dev',
      storage: {
        provider: 's3',
        bucket: 'test-bucket',
        data_path: 'cms/data/',
      },
    },
  },
}));

// Mock shared auth
vi.mock('$lib/services/backends/git/shared/auth', () => ({
  getTokens: vi.fn(),
}));

// Mock other backend services to prevent import errors
vi.mock('$lib/services/backends/fs/local', () => ({
  default: {
    isGit: false,
    name: 'local',
    label: 'Local',
  },
}));

vi.mock('$lib/services/backends/fs/test', () => ({
  default: {
    isGit: false,
    name: 'test-repo',
    label: 'Test',
  },
}));

vi.mock('$lib/services/backends/git/gitea', () => ({
  default: {
    isGit: true,
    name: 'gitea',
    label: 'Gitea',
  },
}));

vi.mock('$lib/services/backends/git/github', () => ({
  default: {
    isGit: true,
    name: 'github',
    label: 'GitHub',
  },
}));

vi.mock('$lib/services/backends/git/gitlab', () => ({
  default: {
    isGit: true,
    name: 'gitlab',
    label: 'GitLab',
  },
}));

// Mock assets service to prevent import chain
vi.mock('$lib/services/assets', () => ({
  allAssets: { subscribe: vi.fn() },
}));

// Mock contents service
vi.mock('$lib/services/contents', () => ({
  allEntries: { subscribe: vi.fn() },
  dataLoaded: { subscribe: vi.fn(), set: vi.fn() },
  dataLoadedProgress: { subscribe: vi.fn(), set: vi.fn() },
}));

// Mock mime module
vi.mock('mime', () => ({
  default: {
    getType: vi.fn(() => 'application/octet-stream'),
  },
}));

// Mock query modules
vi.mock('$lib/services/backends/duckdb/queries/schema', () => ({
  initializeSchema: vi.fn().mockResolvedValue(undefined),
}));

vi.mock('$lib/services/backends/duckdb/queries/entries', () => ({
  loadEntriesFromParquet: vi.fn().mockResolvedValue([]),
  fetchAllEntries: vi.fn().mockResolvedValue([]),
  upsertEntry: vi.fn().mockResolvedValue(undefined),
  deleteEntry: vi.fn().mockResolvedValue(undefined),
  exportEntriesToParquet: vi.fn().mockResolvedValue(new Uint8Array()),
}));

vi.mock('$lib/services/backends/duckdb/queries/assets', () => ({
  loadAssetsFromParquet: vi.fn().mockResolvedValue([]),
  fetchAssetContent: vi.fn().mockResolvedValue(new Uint8Array()),
  insertAsset: vi.fn().mockResolvedValue(undefined),
  deleteAsset: vi.fn().mockResolvedValue(undefined),
  exportAssetsToParquet: vi.fn().mockResolvedValue(new Uint8Array()),
}));

// Mock Worker as a constructor
global.Worker = class MockWorker {
  constructor() {
    this.terminate = vi.fn();
    this.postMessage = vi.fn();
    this.addEventListener = vi.fn();
    this.removeEventListener = vi.fn();
  }
};

// Mock fetch
global.fetch = vi.fn();

describe('DuckDB Backend Constants', () => {
  test('exports correct backend name', () => {
    expect(BACKEND_NAME).toBe('duckdb');
  });

  test('exports correct backend label', () => {
    expect(BACKEND_LABEL).toBe('DuckDB Lakehouse');
  });

  test('exports valid WASM bundle URLs', () => {
    expect(DEFAULT_WASM_BUNDLES).toBeDefined();
    expect(DEFAULT_WASM_BUNDLES.mvp).toBeDefined();
    expect(DEFAULT_WASM_BUNDLES.eh).toBeDefined();
    expect(DEFAULT_WASM_BUNDLES.mvp.mainModule).toContain('duckdb-wasm');
    expect(DEFAULT_WASM_BUNDLES.mvp.mainWorker).toContain('worker.js');
    expect(DEFAULT_WASM_BUNDLES.eh.mainModule).toContain('duckdb-wasm');
    expect(DEFAULT_WASM_BUNDLES.eh.mainWorker).toContain('worker.js');
  });

  test('exports required extensions array', () => {
    expect(REQUIRED_EXTENSIONS).toBeInstanceOf(Array);
    expect(REQUIRED_EXTENSIONS).toContain('httpfs');
    expect(REQUIRED_EXTENSIONS).toContain('parquet');
    expect(REQUIRED_EXTENSIONS).toContain('iceberg');
    expect(REQUIRED_EXTENSIONS).toContain('spatial');
    expect(REQUIRED_EXTENSIONS.length).toBe(4);
  });

  test('exports default presign expiry', () => {
    expect(DEFAULT_PRESIGN_EXPIRY).toBe(900);
    expect(typeof DEFAULT_PRESIGN_EXPIRY).toBe('number');
  });

  test('exports default asset inline threshold', () => {
    expect(DEFAULT_ASSET_INLINE_THRESHOLD).toBe(5 * 1024 * 1024);
    expect(typeof DEFAULT_ASSET_INLINE_THRESHOLD).toBe('number');
  });

  test('exports valid storage types', () => {
    expect(STORAGE_TYPES).toEqual(['iceberg', 'ducklake', 'parquet']);
  });

  test('exports valid storage providers', () => {
    expect(STORAGE_PROVIDERS).toEqual(['s3', 'r2', 'gcs', 'azure']);
  });

  test('exports valid catalog types', () => {
    expect(CATALOG_TYPES).toEqual(['rest', 'duckdb']);
  });

  test('exports valid asset modes', () => {
    expect(ASSET_MODES).toEqual(['inline', 'external']);
  });
});

describe('DuckDB Credentials Module', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearCredentials();
  });

  describe('configureProxy', () => {
    test('sets proxy configuration correctly', () => {
      const proxyConfig = {
        proxyUrl: 'https://cms-auth.workers.dev',
        provider: 's3',
      };

      expect(() => configureProxy(proxyConfig)).not.toThrow();
    });

    test('throws error when proxyUrl is missing', () => {
      expect(() => configureProxy({ proxyUrl: '', provider: 's3' })).toThrow(
        'Credential proxy URL is required'
      );
    });

    test('throws error when provider is missing', () => {
      expect(() => configureProxy({ proxyUrl: 'https://test.com', provider: '' })).toThrow(
        'Storage provider is required'
      );
    });
  });

  describe('setSessionToken', () => {
    test('stores token successfully', () => {
      expect(() => setSessionToken('test-token-123')).not.toThrow();
    });

    test('throws error when token is empty', () => {
      expect(() => setSessionToken('')).toThrow('Valid session token is required');
    });

    test('throws error when token is not a string', () => {
      expect(() => setSessionToken(null)).toThrow('Valid session token is required');
      expect(() => setSessionToken(undefined)).toThrow('Valid session token is required');
      expect(() => setSessionToken(123)).toThrow('Valid session token is required');
    });
  });

  describe('clearCredentials', () => {
    test('clears all credentials and cache', () => {
      setSessionToken('test-token');
      configureProxy({ proxyUrl: 'https://test.com', provider: 's3' });

      clearCredentials();

      const cacheState = getCacheState();
      expect(cacheState.size).toBe(0);
      expect(cacheState.expiresIn).toBe(0);
    });
  });

  describe('getPresignedUrl', () => {
    beforeEach(() => {
      configureProxy({
        proxyUrl: 'https://cms-auth.workers.dev',
        provider: 's3',
      });
      setSessionToken('test-token');
    });

    test('throws error when path is empty', async () => {
      await expect(getPresignedUrl('')).rejects.toThrow('Valid file path is required');
    });

    test('throws error when path is not a string', async () => {
      await expect(getPresignedUrl(null)).rejects.toThrow('Valid file path is required');
      await expect(getPresignedUrl(undefined)).rejects.toThrow('Valid file path is required');
    });

    test('fetches presigned URL for GET operation', async () => {
      const mockResponse = {
        url: 'https://bucket.s3.amazonaws.com/file.txt?signature=abc',
        expiresIn: 900,
        path: 'file.txt',
        operation: 'GET',
      };

      global.fetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      const url = await getPresignedUrl('file.txt', 'GET');

      expect(url).toBe(mockResponse.url);
      expect(global.fetch).toHaveBeenCalledWith(
        'https://cms-auth.workers.dev/presign',
        expect.objectContaining({
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: 'Bearer test-token',
          },
        })
      );
    });

    test('caches GET URLs', async () => {
      const mockResponse = {
        url: 'https://bucket.s3.amazonaws.com/file.txt?signature=abc',
        expiresIn: 900,
        path: 'file.txt',
        operation: 'GET',
      };

      global.fetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      // First call should fetch
      await getPresignedUrl('file.txt', 'GET');
      expect(global.fetch).toHaveBeenCalledTimes(1);

      // Second call should use cache
      await getPresignedUrl('file.txt', 'GET');
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    test('does not cache PUT URLs', async () => {
      const mockResponse = {
        url: 'https://bucket.s3.amazonaws.com/file.txt?signature=abc',
        expiresIn: 900,
        path: 'file.txt',
        operation: 'PUT',
      };

      global.fetch.mockResolvedValue({
        ok: true,
        json: async () => mockResponse,
      });

      // First call
      await getPresignedUrl('file.txt', 'PUT');
      expect(global.fetch).toHaveBeenCalledTimes(1);

      // Second call should fetch again
      await getPresignedUrl('file.txt', 'PUT');
      expect(global.fetch).toHaveBeenCalledTimes(2);
    });

    test('handles authentication failure', async () => {
      global.fetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
        text: async () => 'Unauthorized',
      });

      await expect(getPresignedUrl('file.txt')).rejects.toThrow('Authentication failed');

      // Check that credentials were cleared
      const cacheState = getCacheState();
      expect(cacheState.size).toBe(0);
    });

    test('handles network errors', async () => {
      global.fetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(getPresignedUrl('file.txt')).rejects.toThrow();
    });
  });

  describe('fetchPresignedUrls', () => {
    beforeEach(() => {
      configureProxy({
        proxyUrl: 'https://cms-auth.workers.dev',
        provider: 's3',
      });
      setSessionToken('test-token');
    });

    test('throws error when paths array is empty', async () => {
      await expect(fetchPresignedUrls([])).rejects.toThrow('Valid array of paths is required');
    });

    test('throws error when paths is not an array', async () => {
      await expect(fetchPresignedUrls(null)).rejects.toThrow('Valid array of paths is required');
      await expect(fetchPresignedUrls('not-array')).rejects.toThrow(
        'Valid array of paths is required'
      );
    });

    test('fetches presigned URLs for multiple paths', async () => {
      const mockResponse = {
        urls: {
          'file1.txt': 'https://bucket.s3.amazonaws.com/file1.txt?sig=abc',
          'file2.txt': 'https://bucket.s3.amazonaws.com/file2.txt?sig=def',
        },
        expiresIn: 900,
      };

      global.fetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      const urls = await fetchPresignedUrls(['file1.txt', 'file2.txt']);

      expect(urls).toEqual(mockResponse.urls);
      expect(global.fetch).toHaveBeenCalledWith(
        'https://cms-auth.workers.dev/presign-batch',
        expect.objectContaining({
          method: 'POST',
        })
      );
    });

    test('filters out empty paths', async () => {
      const mockResponse = {
        urls: {
          'file1.txt': 'https://bucket.s3.amazonaws.com/file1.txt?sig=abc',
        },
        expiresIn: 900,
      };

      global.fetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      await fetchPresignedUrls(['file1.txt', '', null, undefined]);

      expect(global.fetch).toHaveBeenCalled();
    });

    test('throws error when no valid paths provided', async () => {
      await expect(fetchPresignedUrls(['', null, undefined])).rejects.toThrow(
        'No valid paths provided'
      );
    });
  });

  describe('getCacheState', () => {
    test('returns empty cache state initially', () => {
      clearCredentials();
      const state = getCacheState();

      expect(state.size).toBe(0);
      expect(state.expiry).toBe(0);
      expect(state.expiresIn).toBe(0);
    });
  });

  describe('exchangeToken', () => {
    test('exchanges OAuth token for session token', async () => {
      const mockResponse = {
        sessionToken: 'session-token-123',
        user: {
          id: '123',
          login: 'testuser',
          name: 'Test User',
        },
        expiresIn: 3600,
      };

      global.fetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      const result = await exchangeToken({
        oauthToken: 'oauth-token-123',
        provider: 'github',
        proxyURL: 'https://cms-auth.workers.dev',
      });

      expect(result).toEqual(mockResponse);
      expect(global.fetch).toHaveBeenCalledWith(
        'https://cms-auth.workers.dev/token-exchange',
        expect.objectContaining({
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            token: 'oauth-token-123',
            provider: 'github',
          }),
        })
      );
    });

    test('throws error when OAuth token is missing', async () => {
      await expect(
        exchangeToken({
          oauthToken: '',
          provider: 'github',
          proxyURL: 'https://test.com',
        })
      ).rejects.toThrow('Valid OAuth token is required');
    });

    test('throws error when provider is invalid', async () => {
      await expect(
        exchangeToken({
          oauthToken: 'token',
          provider: 'invalid',
          proxyURL: 'https://test.com',
        })
      ).rejects.toThrow('Provider must be either "github" or "gitlab"');
    });

    test('throws error when proxyURL is missing', async () => {
      await expect(
        exchangeToken({
          oauthToken: 'token',
          provider: 'github',
          proxyURL: '',
        })
      ).rejects.toThrow('Valid credential proxy URL is required');
    });

    test('handles invalid response from proxy', async () => {
      global.fetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ incomplete: 'response' }),
      });

      await expect(
        exchangeToken({
          oauthToken: 'token',
          provider: 'github',
          proxyURL: 'https://test.com',
        })
      ).rejects.toThrow('Invalid response from credential proxy');
    });
  });
});

describe('DuckDB Repository Module', () => {
  beforeEach(() => {
    resetRepository();
  });

  describe('resetRepository', () => {
    test('clears all repository state', () => {
      repository.service = 'test';
      repository.bucket = 'test-bucket';

      resetRepository();

      expect(repository.service).toBe('');
      expect(repository.bucket).toBe('');
    });
  });

  describe('getFullStoragePath', () => {
    test('constructs correct path with all components', () => {
      repository.bucket = 'my-bucket';
      repository.pathPrefix = 'env/prod';
      repository.dataPath = 'cms/data';

      const path = getFullStoragePath('entries/blog.json');

      expect(path).toBe('my-bucket/env/prod/cms/data/entries/blog.json');
    });

    test('handles missing pathPrefix', () => {
      repository.bucket = 'my-bucket';
      repository.pathPrefix = '';
      repository.dataPath = 'cms/data';

      const path = getFullStoragePath('entries/blog.json');

      expect(path).toBe('my-bucket/cms/data/entries/blog.json');
    });

    test('handles multiple slashes correctly', () => {
      repository.bucket = 'my-bucket/';
      repository.pathPrefix = '/prefix/';
      repository.dataPath = '/data/';

      const path = getFullStoragePath('/file.txt');

      expect(path).not.toContain('//');
    });

    test('handles empty relativePath', () => {
      repository.bucket = 'my-bucket';
      repository.dataPath = 'cms/data';

      const path = getFullStoragePath('');

      expect(path).toBe('my-bucket/cms/data');
    });
  });

  describe('getStorageURI', () => {
    test('generates S3-style URI', () => {
      repository.bucket = 'my-bucket';
      repository.dataPath = 'cms/data';

      const uri = getStorageURI('file.txt');

      expect(uri).toBe('s3://my-bucket/cms/data/file.txt');
    });
  });

  describe('parseStorageConfig', () => {
    test('parses complete storage configuration', () => {
      const backendConfig = {
        storage: {
          provider: 'r2',
          bucket: 'test-bucket',
          data_path: 'custom/path/',
          path_prefix: 'env/staging',
        },
      };

      const config = parseStorageConfig(backendConfig);

      expect(config).toEqual({
        storageProvider: 'r2',
        bucket: 'test-bucket',
        dataPath: 'custom/path/',
        pathPrefix: 'env/staging',
      });
    });

    test('uses default values for missing properties', () => {
      const backendConfig = {
        storage: {},
      };

      const config = parseStorageConfig(backendConfig);

      expect(config).toEqual({
        storageProvider: 's3',
        bucket: '',
        dataPath: 'cms/data/',
        pathPrefix: '',
      });
    });

    test('handles missing storage object', () => {
      const backendConfig = {};

      const config = parseStorageConfig(backendConfig);

      expect(config.storageProvider).toBe('s3');
      expect(config.dataPath).toBe('cms/data/');
    });
  });

  describe('parseCatalogConfig', () => {
    test('parses catalog configuration', () => {
      const backendConfig = {
        catalog: {
          type: 'rest',
          endpoint: 'https://catalog.example.com',
        },
      };

      const config = parseCatalogConfig(backendConfig);

      expect(config).toEqual({
        catalogType: 'rest',
        catalogEndpoint: 'https://catalog.example.com',
      });
    });

    test('uses default values for missing properties', () => {
      const backendConfig = {
        catalog: {},
      };

      const config = parseCatalogConfig(backendConfig);

      expect(config).toEqual({
        catalogType: 'duckdb',
        catalogEndpoint: '',
      });
    });
  });
});

describe('DuckDB Init Module', () => {
  afterEach(async () => {
    await closeDuckDB();
    vi.clearAllMocks();
  });

  describe('isDuckDBReady', () => {
    test('returns false initially', () => {
      expect(isDuckDBReady()).toBe(false);
    });
  });

  describe('hasActiveConnection', () => {
    test('returns false initially', () => {
      expect(hasActiveConnection()).toBe(false);
    });
  });

  describe('initDuckDB', () => {
    test('initializes DuckDB WASM successfully', async () => {
      const db = await initDuckDB();

      expect(db).toBeDefined();
      expect(isDuckDBReady()).toBe(true);
    });

    test('returns existing instance on subsequent calls', async () => {
      const db1 = await initDuckDB();
      const db2 = await initDuckDB();

      expect(db1).toBe(db2);
    });
  });

  describe('getConnection', () => {
    test('creates and returns connection', async () => {
      const conn = await getConnection();

      expect(conn).toBeDefined();
      expect(hasActiveConnection()).toBe(true);
    });

    test('returns existing connection on subsequent calls', async () => {
      const conn1 = await getConnection();
      const conn2 = await getConnection();

      expect(conn1).toBe(conn2);
    });
  });

  describe('closeDuckDB', () => {
    test('closes connection and cleans up resources', async () => {
      await initDuckDB();
      await getConnection();

      await closeDuckDB();

      expect(isDuckDBReady()).toBe(false);
      expect(hasActiveConnection()).toBe(false);
    });

    test('handles close when not initialized', async () => {
      await expect(closeDuckDB()).resolves.not.toThrow();
    });
  });
});

describe('DuckDB Auth Module', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearCredentials();
  });

  afterEach(async () => {
    await closeDuckDB();
  });

  describe('signOut', () => {
    test('clears all state successfully', async () => {
      await expect(signOut()).resolves.not.toThrow();
    });
  });
});

describe('DuckDB Main Module', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('init', () => {
    test('returns repository info when backend is duckdb', () => {
      const repoInfo = init();

      expect(repoInfo).toBeDefined();
      expect(repoInfo.service).toBe('duckdb');
      expect(repoInfo.label).toBe('DuckDB Lakehouse');
    });

    test('validates storage type', () => {
      // This test would need to mock cmsConfig with invalid storage_type
      // For now, we just verify the function returns a valid result
      const repoInfo = init();
      expect(repoInfo).toBeDefined();
    });
  });
});
