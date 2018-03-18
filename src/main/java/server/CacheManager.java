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
public class CacheManager {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private static CacheManager instance;

    private final MemcachedFixedCount fixedCountCache;
    private final MemcachedFixedSizeStrictLRU fixedSizeStrictLRUCache;
    private final MemcachedFixedSizeSlabLRU fixedSizeSlabLRUCache;
    private final MemcachedFixedSizeSlabLRUPages fixedSizeSlabLRUPagesCache;

    private static final long Ki = 1024L;
    private static final long Mi = 1024L * Ki;
    //	private static final long Gi = 1024L * Mi;

    /** maximum number of cache entries */
    private static final long maxCacheSize = 160 * Mi;

    private CacheManager() {
        fixedCountCache = new MemcachedFixedCount(maxCacheSize);
        fixedSizeStrictLRUCache = new MemcachedFixedSizeStrictLRU(
                maxCacheSize);
        fixedSizeSlabLRUCache = new MemcachedFixedSizeSlabLRU(maxCacheSize);
        fixedSizeSlabLRUPagesCache =
                MemcachedFixedSizeSlabLRUPages.getInstance(maxCacheSize);
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
