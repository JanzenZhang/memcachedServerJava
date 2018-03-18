/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Implementation of Cache with fixed size in bytes.
 * Strict LRU access order results in sub-optimal cache because differently
 * sized cache entries could result in larger than necessary number of
 * evictions.
 * In this variation, cache will have a cap on size in bytes but not on
 * the number of items.
 * 
 * Cache is broken down into multiple slabs of fixed preconfigured size in
 * bytes. LRU access order is used within a slab.
 * 
 * Future work: If a slab is full, slab-relocator will asynchronously resize
 * the slabs accordingly (Either unused lower granular slabs are compacted or
 * higher granular slabs are split). With the current framework, this
 * relocation logic will result in very expensive map rehashing.
 * 
 * In order to help maintain an efficient slab-relocator, slab sizes are chosen
 * in powers of 2.
 */
public class MemcachedFixedSizeSlabLRU implements Cache {
    private TreeMap<Integer, MemcachedFixedSizeStrictLRU> cacheMapSlabs;
    private int highestCacheMapKey;

    /** HARD LIMIT on cache size in number of bytes */
    private final long maxGlobalCacheSize;

    private final int maxSlabs = 10;

    ListeningExecutorService service;

    /**
     * Number of threads available to run get requests on all Slabs.
     * TODO: Carefully craft the threadPoolSize to divide between cacheManager
     * and ConnectionManager as we have a mixture of IO heavy and CPU heavy
     * activity between these components.
     */
    private static final int threadPoolSize =
            Runtime.getRuntime().availableProcessors();

    public MemcachedFixedSizeSlabLRU(final long maxCacheSize) {
        this.maxGlobalCacheSize = maxCacheSize;
        cacheMapSlabs = new TreeMap<>();
        service = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(threadPoolSize));

        initializeSlabs();
    }

    private void initializeSlabs() {
        // Insert fixed number of entries into this map.
        // Let the keys be powers of 2 and let consecutive keys differ by a
        // factor of 4. Fill from sizes of 16B to 4 MiB. We will have in total
        // 10 such slabs, with equal sizes. So ensure that maxCacheSize is a
        // multiple of 10.
        assert(maxGlobalCacheSize % maxSlabs == 0);
        final long slabSize = maxGlobalCacheSize / maxSlabs;
        for (int i=4; i<=22; i+=2) {
            int key = (int) Math.pow(2, i);
            MemcachedFixedSizeStrictLRU slab =
                    new MemcachedFixedSizeStrictLRU(slabSize);
            cacheMapSlabs.put(key, slab);
        }
        this.highestCacheMapKey = (int) Math.pow(2, 22);
        assert(cacheMapSlabs.size() == maxSlabs);
    }

    private MemcachedFixedSizeStrictLRU getCacheSlab(final int size) {
        Integer key = cacheMapSlabs.ceilingKey(size);
        if (key == null) {
            key = highestCacheMapKey;
        }
        return cacheMapSlabs.get(key);
    }

    private long getCurrentCacheSize() {
        long currentGlobalCacheSize = 0;
        for (MemcachedFixedSizeStrictLRU cacheSlab : cacheMapSlabs.values()) {
            currentGlobalCacheSize += cacheSlab.getCurrentCacheSize();
        }
        return currentGlobalCacheSize;
    }

    @Override
    public CacheValue get(String key) throws InterruptedException {
        assert (key != null);

        // First find the cache Slab that could hold this key. Since we don't
        // know the size of value, we have to broadcast the request to all
        // slabs.

        CacheValue retValue = null;
        List<Future<CacheValue>> futureList = new LinkedList<>();
        CompletionService<CacheValue> completionService =
                new ExecutorCompletionService<>(service);
        // Ask all SlabCaches if they have this key.
        try {
            int taskCount = cacheMapSlabs.size();
            for (MemcachedFixedSizeStrictLRU slabCache : cacheMapSlabs.values()) {
                Future<CacheValue> task = completionService.submit(
                        new Callable<CacheValue>() {
                            public CacheValue call() {
                                return slabCache.get(key);
                            }
                        }
                );
                futureList.add(task);
            }

            /* Only one of the futures will have the required cacheValue if any.
             * As soon as one returns, cancel other futures and return the cached
             * value.
             */
            for (int i = 0; i < taskCount; ++i) {
                try {
                    Future<CacheValue> fut = completionService.take();
                    CacheValue value = fut.get();
                    if (value != null) {
                        retValue = value;
                        break;
                    }
                } catch (ExecutionException ignore) {
                    ignore.printStackTrace();
                }
            }
        } finally {
            for (Future<CacheValue> f : futureList) {
                f.cancel(true);
            }
        }
        return retValue;

    }

    @Override
    public boolean set(String key, CacheValue value) {
        assert (key != null);
        assert (value != null);

        // First find the cache slab that could hold this key.
        MemcachedFixedSizeStrictLRU cacheSlab = getCacheSlab(
                value.getSerializedSize());
        assert (cacheSlab != null);
        return cacheSlab.set(key, value);
    }
}
