/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of Cache with fixed size in bytes and LRU access
 * order. In this instance, cache will have a cap on size in bytes but not on
 * the number of items. Therefore, it can have a open bound on cache metadata
 * like number of buckets.
 */
public class MemcachedFixedSizeStrictLRU implements Cache {
    private ConcurrentHashMap<String, CacheValue> cache;
    // Stores references to keys in map, ordered by least recently used
    // accesses.
    private Queue<String> lruKeyList;

    /** HARD LIMIT on cache size in number of bytes */
    private final long maxCacheSize;

    /** Used cache size in number of bytes */
    private long currentCacheSize;

    public MemcachedFixedSizeStrictLRU(final long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.currentCacheSize = 0;
        this.cache = new ConcurrentHashMap<>();
        this.lruKeyList = new LinkedList<>();
    }

    public long getCurrentCacheSize() {
        return currentCacheSize;
    }

    @Override
    public CacheValue get(String key) {
        assert(key != null);

        CacheValue value = cache.get(key);

        if (value != null) {
            synchronized(lruKeyList) {
                // Reinsert the key at head.
                lruKeyList.remove(key);
                // this key may not be found in LRUKeyList momentarily because
                // a different thread on set() could have recycled it.
                lruKeyList.add(key);
            }
        }
        return value;
    }

    @Override
    public boolean set(String key, CacheValue value) {
        assert(key != null);
        assert(value != null);
        assert(value.getSerializedSize() <= maxCacheSize);

        // Keep updating a local copy of size of garbage collected (gc) cache
        // elements. This helps in indirect locking of currentCacheSize.
        long gcCacheSize = 0;
        while (currentCacheSize - gcCacheSize + value.getSerializedSize() >
                maxCacheSize) {
            final String rKey;
            synchronized(lruKeyList) {
                rKey = lruKeyList.remove();
            }
            CacheValue rValue = cache.remove(rKey);
            gcCacheSize += rValue.getSerializedSize();
        }

        CacheValue prevValue = cache.put(key, value);
        synchronized(lruKeyList) {
            if (prevValue != null) {
                // remove previous reference to this key in LRU list and reinsert
                // it to queue.
                boolean isRemove = lruKeyList.remove(key);
                assert(isRemove == true);
            }
            lruKeyList.add(key);
        }

        currentCacheSize = currentCacheSize - gcCacheSize +
                value.getSerializedSize();
        synchronized(lruKeyList) {
            assert(lruKeyList.size() == cache.size());
        }
        return true;
    }
}
