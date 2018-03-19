/**
 * @author Dilip Simha
 */
package server.cache;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * SlabCache maintains the cache for a given slab.
 * For new set requests, it tried to grow cache until pageManager runs out of
 * memory. Beyond that it maintains its own LRU list and recycles within its
 * pages.
 */
public final class SlabCache implements Cache {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private final Slab slab;

    private final ConcurrentHashMap<String, CacheSlot> slabCacheMap;

    // Stores references to keys in map, ordered by least recently used
    // accesses.
    private Queue<String> lruKeyList;

    private Semaphore lruKeyListSemaphore;

    public SlabCache(final int slotSize, final PageManager pageManager) {
        this.slab = new Slab(slotSize, pageManager);
        this.slabCacheMap = new ConcurrentHashMap<>();
        this.lruKeyList = new LinkedList<>();
        this.lruKeyListSemaphore = new Semaphore(1);

        LOGGER.info("Slabcache instance created: " + slotSize);
    }

    @Override
    public CacheValue get(final String key) throws InterruptedException {
        LOGGER.info("slabcache: " + getSlotSize() + " size: "
                + slabCacheMap.size());

        CacheSlot cacheSlot;
        synchronized (slabCacheMap) {
            cacheSlot = slabCacheMap.get(key);
            if (cacheSlot == null) {
                LOGGER.info("key: " + key + " not found in slabcache: "
                        + getSlotSize());
                return null;
            }
            cacheSlot.lock();
        }

        lruKeyListSemaphore.acquire();

        // Reinsert the key at head.
        boolean isRemove = lruKeyList.remove(key);
        assert (isRemove);
        lruKeyList.add(key);

        lruKeyListSemaphore.release();

        final Slab slab1 = cacheSlot.getSlab();
        assert (slab1 == this.slab);
        final Page page = cacheSlot.getPage();
        final int offset = cacheSlot.getOffset();

        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(offset);
        buf.limit(offset + slab.getSlotSize());
        try {
            CacheValue cacheValue = CacheValue.deserialize(buf);
            LOGGER.info("key: " + key + " found in slabcache: "
                    + getSlotSize());
            return cacheValue;
        } catch (BufferUnderflowException e) {
            LOGGER.severe(e.getMessage());
            return null;
        } finally {
            cacheSlot.unlock();
        }
    }

    @Override
    public boolean set(final String key, final CacheValue value)
            throws InterruptedException {
        assert (key != null);
        assert (value != null);
        assert (value.getSerializedSize() <= getSlotSize());

        CacheSlot cacheSlot;
        synchronized (slabCacheMap) {
            cacheSlot = slabCacheMap.get(key);

            // reuse the cacheSlot to store new value.
            if (cacheSlot == null) {
                // Get either a new CacheSlot or reuse one from LRU.
                cacheSlot = slab.getSlot();
                if (cacheSlot == null) {
                    LOGGER.info("LRU kicked in slab: " + getSlotSize());
                    // All memory exhausted. Evict one and reuse it.
                    lruKeyListSemaphore.acquire();
                    if (lruKeyList.isEmpty()) {
                        // This can happen when other slabs have taken up all
                        // the required memory before even the first request is
                        // made on this slab.
                        LOGGER.info("SlabCache set failing to cache"
                                + " because of lack of memory. Key: " + key);
                        lruKeyListSemaphore.release();
                        return false;
                    }

                    final String rKey = lruKeyList.remove();
                    cacheSlot = slabCacheMap.remove(rKey);
                    lruKeyListSemaphore.release();
                }
            }
            assert (cacheSlot != null);
            cacheSlot.lock();
        }

        // Use the cacheSlot to store new value.
        final Slab slab1 = cacheSlot.getSlab();
        assert (slab1 == this.slab);
        final Page page = cacheSlot.getPage();
        final int offset = cacheSlot.getOffset();
        assert (offset + slab.getSlotSize() <= page.getPageSize());

        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        buf.position(offset);
        buf.limit(offset + slab.getSlotSize());
        try {
            CacheValue.serialize(value, buf);
            LOGGER.info("Key: " + key + " set in slabCache: " + getSlotSize());
            LOGGER.info("Size of slabcache: " + slabCacheMap.size());
            return true;
        } catch (BufferOverflowException e) {
            LOGGER.severe(e.getMessage());
            return false;
        } finally {
            synchronized (slabCacheMap) {
                lruKeyList.add(key);
                slabCacheMap.put(key, cacheSlot);
            }
            cacheSlot.unlock();
        }
    }

    @VisibleForTesting
    int getCacheSize() {
        assert (slabCacheMap.size() == lruKeyList.size());

        return slabCacheMap.size();
    }

    @VisibleForTesting
    int getSlotSize() {
        return slab.getSlotSize();
    }
}
