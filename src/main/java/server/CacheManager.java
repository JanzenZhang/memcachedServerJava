/**
 * @author Dilip Simha
 */
package server;

import java.util.logging.Logger;

import server.cache.Cache;
import server.cache.MemcachedFixedCount;
import server.cache.MemcachedFixedSizeSlabLRU;
import server.cache.MemcachedFixedSizeSlabLRUPages;
import server.cache.MemcachedFixedSizeStrictLRU;

/**
 * Cache manager to manage specific implementation of cache. Its a singleton.
 */
public final class CacheManager {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private static CacheManager instance;

    private final MemcachedFixedCount fixedCountCache;
    private final MemcachedFixedSizeStrictLRU fixedSizeStrictLRUCache;
    private final MemcachedFixedSizeSlabLRU fixedSizeSlabLRUCache;
    private final MemcachedFixedSizeSlabLRUPages fixedSizeSlabLRUPagesCache;

    private static final long K = 1024L;
    private static final long M = 1024L * K;

    /** maximum number of cache entries. */
    private static final long MAX_CACHE_SIZE = 160 * M;

    private CacheManager() {
        fixedCountCache = new MemcachedFixedCount(MAX_CACHE_SIZE);
        fixedSizeStrictLRUCache = new MemcachedFixedSizeStrictLRU(
                MAX_CACHE_SIZE);
        fixedSizeSlabLRUCache = new MemcachedFixedSizeSlabLRU(MAX_CACHE_SIZE);
        fixedSizeSlabLRUPagesCache =
                MemcachedFixedSizeSlabLRUPages.getInstance(MAX_CACHE_SIZE);
        LOGGER.finer("CacheManager singleton instance created");
    }

    public static CacheManager getInstance() {
        if (instance == null) {
            instance = new CacheManager();
        }
        return instance;
    }

    public Cache getCache() {
        return fixedSizeSlabLRUPagesCache;
    }
}
