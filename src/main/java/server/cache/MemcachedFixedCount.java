/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Implementation of Cache with fixed count and LRU access order.
 * In this instance, cache will have a cap on number of items but not on the
 * storage used. Therefore, it can have a open bound on storage requirement.
 */
public final class MemcachedFixedCount implements Cache {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private static MemcachedFixedCount instance;

    private Map<String, CacheValue> cache;

    /** HARD LIMIT on cache size in number of entries. */
    private final long maxCacheSize;

    /** Used cache size in number of entries. */
    private long currentCacheSize;

    private MemcachedFixedCount(final long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.currentCacheSize = 0;
        cache = new LinkedHashMap<String, CacheValue>(101, 0.75f, true) {
            private static final long serialVersionUID = -1519067336368084307L;
            protected boolean removeEldestEntry(
                    final Map.Entry<String, CacheValue> eldest) {
                if (currentCacheSize == maxCacheSize) {
                    LOGGER.finest("Cache remove: key: " + eldest.getKey()
                        + " size: " + currentCacheSize
                            + " value:" + eldest.getValue().getFlag());
                    return true;
                }
                return false;
            }
        };
        LOGGER.info("Created MemcachedFixedCount for size: " + maxCacheSize);
    }

    public static MemcachedFixedCount getInstance(final long maxCacheSize) {
        if (instance == null) {
            instance = new MemcachedFixedCount(maxCacheSize);
        }
        assert (instance.maxCacheSize == maxCacheSize);
        LOGGER.finest("singleton instance created");

        return instance;
    }

    @Override
    public synchronized CacheValue get(final String key) {
        assert (key != null);

        final CacheValue value = cache.get(key);
        LOGGER.finest("Cache get: key: " + key + " value:" + value);
        return value;
    }

    @Override
    public synchronized boolean set(final String key, final CacheValue value) {
        assert (key != null);
        assert (value != null);

        cache.put(key, value);
        LOGGER.finest("Cache put: key: " + key + " size: " + currentCacheSize
                + " value:" + value);

        if (currentCacheSize < maxCacheSize) {
            currentCacheSize++;
        }
        return true;
    }

    public long getCurrentCacheSize() {
        return currentCacheSize;
    }

    public long getMaxCacheSize() {
        return maxCacheSize;
    }
}
