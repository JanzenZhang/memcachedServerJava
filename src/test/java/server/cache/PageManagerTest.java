/**
 * 
 */
package server.cache;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import server.cache.Page;
import server.cache.PageManager;

/**
 * Test PageManager.
 *
 * @author Dilip Simha
 */
public class PageManagerTest {
    PageManager pageManager;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    /**
     * Test method for {@link server.cache.PageManager#getPage()}.
     * Test for no pages in pool.
     */
    @Test
    public final void testGetZeroPage() {
        final long maxGlobalCacheSize = 16 * 1024 * 1024 - 1;
        thrown.expect(IllegalArgumentException.class);
        PageManager pageManager = PageManager.getInstance(maxGlobalCacheSize);
        pageManager.getPage();
    }

    /**
     * Test method for {@link server.cache.PageManager#getPage()}.
     * Test for single page in pool.
     */
    @Test
    public final void testGetSinglePage() {
        final long maxGlobalCacheSize = 16 * 1024 * 1024;
        PageManager pageManager = PageManager.getInstance(maxGlobalCacheSize);
        Page page = pageManager.getPage();
        assert (page != null);
        page = pageManager.getPage();
        assert (page == null);
    }

    /**
     * Test method for {@link server.cache.PageManager#getPage()}.
     * Test for single page in pool.
     */
    @Test
    public final void testGetMultiplePagesParallel()
            throws InterruptedException, ExecutionException {
        final int pageCount = 10;
        final int pageSize = 16 * 1024 * 1024;
        final long maxGlobalCacheSize = pageSize * pageCount;
        PageManager pageManager = PageManager.getInstance(maxGlobalCacheSize);

        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        AtomicInteger pageFetchCount = new AtomicInteger();

        Runnable task = new Runnable() {
            @Override
            public void run() {
                Page page = pageManager.getPage();
                int count = pageFetchCount.incrementAndGet();
                if (page != null && count > pageCount) {
                    fail("page should have been null");
                } else if (page == null && count <= pageCount) {
                    fail("page should not have been null: count: " +  count +
                            " MaxPageCount: " + pageCount);
                }
            }
        };

        for (int i = 0; i < pageCount + 2; i++) {
            futureList.add(service.submit(task));
        }

        for (Future<?> f : futureList) {
            f.get();
        }
    }
}
