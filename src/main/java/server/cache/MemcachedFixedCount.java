/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of Cache with fixed count and LRU access order.
 * In this instance, cache will have a cap on number of items but not on the
 * storage used. Therefore, it can have a open bound on storage requirement.
 */
public class MemcachedFixedCount implements Cache {
    private Map<String, CacheValue> cache;

    /** HARD LIMIT on cache size in number of entries */
    private final long maxCacheSize;

    /** Used cache size in number of entries */
    private long currentCacheSize;

    public MemcachedFixedCount(final long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.currentCacheSize = 0;
        cache = new LinkedHashMap<String, CacheValue>(101, 0.75f, true) {
            private static final long serialVersionUID = -1519067336368084307L;
            protected boolean removeEldestEntry(
                    Map.Entry<String, CacheValue> eldest) {
                if (currentCacheSize == maxCacheSize) {
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    public CacheValue get(String key) {
        assert(key != null);

        return cache.get(key);
    }

    @Override
    public boolean set(String key, CacheValue value) {
        assert(key != null);
        assert(value != null);

        cache.put(key, value);

        if (currentCacheSize < maxCacheSize) {
            currentCacheSize = maxCacheSize;
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
