/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

/**
 * Cache Slot to hold data. This is a fixed memory sized object in a Page.
 * Accesses to CacheSlot must be synchronized using lock and unlock functions
 * in this class.
 * Before locking a cache slot, take lock on slabcache's map.
 */
public final class CacheSlot {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    /** The slab that owns this cacheSlot. */
    private final Slab slab;

    /** The page that contains this cacheSlot. */
    private final Page page;

    /** Offset of data in its page. */
    private final int offset;

    /** Mutex to control access to this entire cache slot. */
    private Semaphore sem;

    public CacheSlot(final Slab slab, final Page page,
            final int offset) {
        this.slab = slab;
        this.page = page;
        this.offset = offset;
        // This semaphore is essentially a mutex.
        this.sem = new Semaphore(1);
    }

    /**
     * Get the slab.
     * @return return the slab for this slot.
     */
    public Slab getSlab() {
        assert (isLocked());
        return slab;
    }

    /**
     *  Get the page.
     * @return page
     */
    public Page getPage() {
        assert (isLocked());
        return page;
    }

    public int getOffset() {
        assert (isLocked());
        return offset;
    }

    public void lock() throws InterruptedException {
        sem.acquire();
        LOGGER.finest("CacheSlot locked: " + this);
    }

    private boolean isLocked() {
        return (sem.availablePermits() == 0);
    }

    public void unlock() {
        assert (isLocked());
        LOGGER.finest("CacheSlot unlocked: " + this);
        sem.release();
    }
}
