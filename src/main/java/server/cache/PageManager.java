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
public class PageManager {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private final long maxGlobalCacheSize;

    /** 16 MiB */
    private static final int pageSize = 16 * 1024 * 1024;

    private final int pagePoolCount;

    private Queue<Page> pagePool;

    private static PageManager instance;

    private PageManager(final long maxGlobalCacheSize) {
        this.maxGlobalCacheSize = maxGlobalCacheSize;
        this.pagePoolCount = (int) (maxGlobalCacheSize / pageSize);
        assert(pagePoolCount > 0);
        this.pagePool = new LinkedList<>();

        for (int p = 0; p < pagePoolCount; p++) {
            Page page = new Page(pageSize);
            pagePool.add(page);
        }
    }

    public static PageManager getInstance(final long maxGlobalCacheSize) {
        Preconditions.checkArgument(maxGlobalCacheSize > pageSize);

        if (instance == null) {
            instance = new PageManager(maxGlobalCacheSize);
        }
        assert(instance.maxGlobalCacheSize == maxGlobalCacheSize);
        LOGGER.info("PageManager singleton instance created");

        return instance;
    }

    /**
     * Get a free page from pool. This is thread-safe.
     * @return new page if available, null otherwise
     */
    public synchronized Page getPage() {
        LOGGER.info("getPage: " + pagePool.size());
        return pagePool.poll();
    }

    public static int getPageSize() {
        return pageSize;
    }
}
