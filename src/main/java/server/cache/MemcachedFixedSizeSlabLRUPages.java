/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Implementation of Cache with fixed size in bytes. Differs from
 * MemcachedFixedSizeSlabLRU in the way cache slots are allocated.
 * In this variation, memory for cache slots are held in units of pages.
 * Each slab maintains its own set of pages and pages within a slab have fixed
 * set of cache slots depending on the cache slot size configured for that
 * slab.
 * Pages are maintained by a central memory manager(PageManager) and once pages
 * are allocated to slabs, they are not reclaimed.
 * Each Slab maintains its own LRU.
 * To limit internal fragmentation, consider revisiting the number of slabs
 * created and difference between slot sizes of consecutive slabs.
 */
public final class MemcachedFixedSizeSlabLRUPages implements Cache {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private static MemcachedFixedSizeSlabLRUPages instance;

    /** HARD LIMIT on cache size in number of bytes */
    private final long maxGlobalCacheSize;

    /**
     * All slabs in this cache are mapped against its size and maintained in a
     * TreeMap. This TreeMap will serve as a basis of choosing which slab
     * should hold a given cache key.
     * We don't need this synchronized because once created at startup time,
     * its always accessed as a read-only source.
     */
    private final TreeMap<Integer, SlabCache> slabTree;

    private static final int MAX_SLABS = 10;

    private final PageManager pageManager;

    private ListeningExecutorService service;

    /**
     * Number of threads available to run get requests on all Slabs.
     * TODO: Carefully craft the threadPoolSize to divide between cacheManager
     * and ConnectionManager as we have a mixture of IO heavy and CPU heavy
     * activity between these components.
     */
    private static final int THREAD_POOL_SIZE =
            Runtime.getRuntime().availableProcessors();

    public static MemcachedFixedSizeSlabLRUPages getInstance(
            final long maxCacheSize) {
        if (instance == null) {
            instance = new MemcachedFixedSizeSlabLRUPages(maxCacheSize);
        }
        assert (instance.maxGlobalCacheSize == maxCacheSize);
        LOGGER.finest("singleton instance created");

        return instance;
    }

    private MemcachedFixedSizeSlabLRUPages(final long maxCacheSize) {
        this.maxGlobalCacheSize = maxCacheSize;

        slabTree = new TreeMap<>();
        this.pageManager = PageManager.getInstance(maxGlobalCacheSize);
        initializeSlabs();
        service = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(THREAD_POOL_SIZE));
        LOGGER.finest("Singleton instance created");
    }

    /** Insert fixed number of entries into slabTree.
     *  Let the keys be powers of 2 and let consecutive keys differ by a
     *  factor of 4. Fill from sizes of 16B to 4 MiB. We will have in total
     *  10 such slabs and each slab can hold arbitrary number of cacheSlots
     *  based on demand (workload dependent).
     */
    private void initializeSlabs() {
        for (int i = 4; i <= 22; i += 2) {
            int slotSize = (int) Math.pow(2, i);
            SlabCache slabCache = new SlabCache(slotSize, pageManager);
            slabTree.put(slotSize, slabCache);
        }
        assert (slabTree.size() == MAX_SLABS);
    }

    private SlabCache getSlabCache(final int size) {
        Integer key = slabTree.ceilingKey(size);
        if (key == null) {
            /* TODO : Break the key into smaller parts and store it in this
             * cache.Note to link all these smaller parts, so that their
             * presence in cache should either be all or none. This requires
             * a lot of sophistication to maintain an internal map for all such
             * mappings and the ability to either pin some cache elements.
             */
            LOGGER.severe("Slab for keySize: " + size
                    + " doesn't exist." + " Maximum slab size: "
                    + slabTree.lastKey());
            return null;
        }
        return slabTree.get(key);
    }

    @Override
    public CacheValue get(final String key) throws InterruptedException {
        assert (key != null);

        CacheValue retValue = null;
        List<Future<CacheValue>> futureList = new LinkedList<>();
        CompletionService<CacheValue> completionService =
                new ExecutorCompletionService<>(service);
        // Ask all SlabCaches if they have this key.
        try {
            int taskCount = slabTree.size();
            for (SlabCache slabcache : slabTree.values()) {
                Future<CacheValue> task = completionService.submit(
                        new Callable<CacheValue>() {
                            public CacheValue call() {
                                try {
                                    return slabcache.get(key);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return null;
                            }
                        }
                );
                futureList.add(task);
            }

            /* Only one of the futures will have the required cacheValue if
             * any. As soon as one returns, cancel other futures and return
             * the cached value.
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

        LOGGER.finer("get for key: " + key + " returned: "
                + (retValue != null));
        return retValue;
    }

    @Override
    public boolean set(final String key, final CacheValue value)
            throws InterruptedException {
        assert (key != null);
        assert (value != null);

        LOGGER.finer("serializedSize: " + value.getSerializedSize());
        // First find the cache Slab that could hold this key.
        SlabCache slabCache = getSlabCache(value.getSerializedSize());
        if (slabCache != null) {
            if (slabCache.set(key, value)) {
                LOGGER.finer("Cached for key: " + key + " in slab: "
                    + slabCache.getSlotSize());
                return true;
            } else {
                LOGGER.finer("Cannot cache for key: " + key + " in slab: "
                        + slabCache.getSlotSize());
                return false;
            }
        } else {
            LOGGER.finer("Cannot cache key: " + key);
            return false;
        }
    }

    @VisibleForTesting
    Collection<SlabCache> getSlabCaches() {
        return slabTree.values();
    }

    @VisibleForTesting
    static int getMaxSlabs() {
        return MAX_SLABS;
    }
}
