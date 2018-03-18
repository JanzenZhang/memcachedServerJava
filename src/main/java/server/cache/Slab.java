/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * Slab maintains pages in its private pool. Pages are fetched from PageManager
 * and once acquired, they aren't released back to generic pool.
 * Each slab is configured of a unique size(power of 2) and all cacheSlots
 * within this slab are of this same slot size.
 * 
 * SlabCache extends each slab into its own mini cache.
 */
public class Slab {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    /**
     * Size of cache slots in bytes. This slab will hold elements of size
     * lesser than slabSize.
     */    
    private final int slotSize;

    private final PageManager pageManager;

    private final int slotsPerPage;

    private boolean isGlobalPoolEmpty;

    /** All pages in this slab hold fixed sized slots of slabSize bytes. */
    List<Page> pages;

    /** List of free cacheSlots */
    Queue<CacheSlot> freeCacheSlotList;

    Slab(final int slotSize, final PageManager pageManager) {
        this.slotSize = slotSize;
        this.pageManager = pageManager;

        this.freeCacheSlotList = new LinkedList<>();
        this.isGlobalPoolEmpty = false;
        this.pages = new LinkedList<>();
        assert(PageManager.getPageSize() % this.slotSize == 0);
        this.slotsPerPage = (int) (PageManager.getPageSize() / this.slotSize);
    }
    
    private void addToFreeSlotOffsetList(Page newPage) {
        for (int i=0; i< slotsPerPage; i++) {
            freeCacheSlotList.add(new CacheSlot(this, newPage, i*slotSize));
        }
    }

    public int getSlotSize() {
        return slotSize;
    }

    /**
     * Get a free cache Slot from this Slab. If a cacheSlot is not readily
     * available, request global PageManager for a new page.
     * @return new cacheSlot to store cache value; null if no cacheSlots
     *          available.
     */
    public synchronized CacheSlot getSlot() {
        LOGGER.info("getSlot : " + slotSize + " freeSLots: " +
                freeCacheSlotList.size());
        if (freeCacheSlotList.isEmpty()) {
            if (!isGlobalPoolEmpty) {
                Page newPage = pageManager.getPage();
                if (newPage != null) {
                    addToFreeSlotOffsetList(newPage);
                } else {
                    LOGGER.info("Global Pool marked empty");
                    isGlobalPoolEmpty = true;
                }
            }
        }
        return freeCacheSlotList.poll();
    }

    /**
     * Return the cache slot to this slab after use.
     * This cache slot is immediately made available for getSlot.
     */
    public synchronized void putSlot(final CacheSlot slot) {
        freeCacheSlotList.add(slot);
    }
}
