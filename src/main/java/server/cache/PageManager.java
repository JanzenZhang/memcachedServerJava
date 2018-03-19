/**
 * @author Dilip Simha
 */
package server.cache;

import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

/**
 * PageManager manages the generic memory pool for entire cache.
 * Slabs can request pages on demand from this pool.
 * Once pages are assigned to a slab, they are not returned to this pool.
 * TODO : Manage page reclamation logic.
 */
public final class PageManager {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private final long maxGlobalCacheSize;

    /** 16 MiB */
    private static final int PAGE_SIZE = 16 * 1024 * 1024;

    private final int pagePoolCount;

    private Queue<Page> pagePool;

    private static PageManager instance;

    private PageManager(final long maxGlobalCacheSize) {
        this.maxGlobalCacheSize = maxGlobalCacheSize;
        this.pagePoolCount = (int) (maxGlobalCacheSize / PAGE_SIZE);
        assert (pagePoolCount > 0);
        this.pagePool = new LinkedList<>();

        for (int p = 0; p < pagePoolCount; p++) {
            Page page = new Page(PAGE_SIZE);
            pagePool.add(page);
        }
    }

    public static PageManager getInstance(final long maxGlobalCacheSize) {
        Preconditions.checkArgument(maxGlobalCacheSize >= PAGE_SIZE);

        if (instance == null) {
            instance = new PageManager(maxGlobalCacheSize);
        }
        assert (instance.maxGlobalCacheSize == maxGlobalCacheSize);
        LOGGER.finest("PageManager singleton instance created");

        return instance;
    }

    /**
     * Get a free page from pool. This is thread-safe.
     * @return new page if available, null otherwise
     */
    public synchronized Page getPage() {
        LOGGER.finest("getPage: " + pagePool.size());
        return pagePool.poll();
    }

    public static int getPageSize() {
        return PAGE_SIZE;
    }
}
