/**
 * @author Dilip Simha
 */
package server.cache;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import server.cache.CacheValue;
import server.cache.PageManager;
import server.cache.SlabCache;

/**
 *
 */
public class SlabCacheTest {
    private final int pageCount = 10;
    private final long maxGlobalCacheSize =
            pageCount * PageManager.getPageSize();
    private final PageManager pageManager = PageManager.getInstance(
            maxGlobalCacheSize);
    private SlabCache slabCache;
    private static final int slotSize = PageManager.getPageSize() / 16;

    /**
     */
    @Before
    public void setUp() {
        slabCache = new SlabCache(slotSize, pageManager);
    }

    /**
     */
    @After
    public void tearDown() {
        slabCache = null;
    }

    /**
     * Test method for {@link server.cache.SlabCache#get(java.lang.String)}.
     */
    @Test
    public final void testGetExists() throws InterruptedException {
        String key = "hello";
        final short flags = 123;
        final int bytes = 10;
        final byte[] data = RandomUtils.nextBytes(bytes);
        
        CacheValue expectedValue = new CacheValue(flags, bytes, data);
        slabCache.set(key, expectedValue);
        
        CacheValue cachedValue = slabCache.get(key);
        if (! expectedValue.equals(cachedValue)) {
            fail("cachedValue: " + cachedValue +
                    " expectedValue: " + expectedValue);
        }
    }

    /**
     * Test method for {@link server.cache.SlabCache#get(java.lang.String)}.
     */
    @Test
    public final void testGetNotExists() throws InterruptedException {
        String key = "hello";
        final short flags = 123;
        final int bytes = 10;
        final byte[] data = RandomUtils.nextBytes(bytes);
        
        CacheValue value = new CacheValue(flags, bytes, data);
        slabCache.set(key, value);
        
        CacheValue dummyValue = slabCache.get(key + "-dummy");
        assert(dummyValue == null);
    }

    private static String generateRandomKey(ArrayList<String> keys) {
        int randomElementIndex = ThreadLocalRandom.current().nextInt(
                keys.size());
        return keys.get(randomElementIndex);
    }

    /**
     * Test method for {@link server.cache.SlabCache#get(java.lang.String)}.
     */
    @Test
    public final void testGetParallel()
            throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        final int chunkCount = 50;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());

        for (int i=0; i<chunkCount; i++) {
            String key = "test-" + i;
            final short flags = (short) i;
            final int bytes = 10;
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue value = new CacheValue(flags, bytes, data);
            slabCache.set(key, value);
            testMap.put(key, value);
        }

        final ArrayList<String> keys = new ArrayList<String>(testMap.keySet());

        Runnable task = new Runnable() {
            @Override
            public void run() {
                String key = generateRandomKey(keys);
                try {
                    CacheValue cachedValue = slabCache.get(key);
                    CacheValue expectedValue = testMap.get(key);
                    if (! expectedValue.equals(cachedValue)) {
                        fail("cachedValue: " + cachedValue +
                                " expectedValue: " + expectedValue);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        for (int i = 0; i < chunkCount * 10; i++) {
            futureList.add(service.submit(task));
        }

        // Block until all futures are done.
        for (Future<?> f : futureList) {
            f.get();
        }
    }

    /**
     * Test set call with large number of inserts such that we test LRU logic.
     * Test method for {@link server.cache.SlabCache#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testSetLRU() throws InterruptedException {
        final int slotsPerPage = PageManager.getPageSize() / slotSize;
        final int chunkCount = slotsPerPage * pageCount;
        HashMap<String, CacheValue> testMap = new HashMap<>();

        for (int i=0; i<chunkCount; i++) {
            String key = "test-" + i;
            final short flags = (short) i;
            final int bytes = 10;
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue value = new CacheValue(flags, bytes, data);
            slabCache.set(key, value);
            testMap.put(key, value);
        }

        // Verify that all keys are present and nothing should be recycled.
        for (int i=0; i<chunkCount; i++) {
            String key = "test-" + i;
            CacheValue cachedValue = slabCache.get(key);
            CacheValue expectedValue = testMap.get(key);
            if (! expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue +
                        " expectedValue: " + expectedValue);
            }
        }

        // Now insert one more into cache and ensure first element is kicked
        // out.
        int chunkIndex = chunkCount;
        String key = "test-" + chunkIndex;
        final short flags = (short) chunkIndex;
        final int bytes = 10;
        final byte[] data = RandomUtils.nextBytes(bytes);

        // Check that latest added element is present in cache.
        CacheValue value = new CacheValue(flags, bytes, data);
        slabCache.set(key, value);
        testMap.put(key, value);

        // Check that first added element is NOT present in cache.
        key = "test-" + 0;
        CacheValue cachedValue = slabCache.get(key);
        if (cachedValue != null) {
            fail("cachedValue: " + cachedValue + " is not null!");
        }
    }

    /**
     * Test for parallel set calls.
     * Test method for {@link server.cache.SlabCache#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testSetParallel()
            throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();

        final int slotsPerPage = PageManager.getPageSize() / slotSize;
        final int chunkCount = slotsPerPage * pageCount;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());

        final int bytes = 10;
        for (int i = 0; i < chunkCount; i++) {
            final String key = "test-" + i;
            final short flags = (short) i;
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        final byte[] data = RandomUtils.nextBytes(bytes);

                        CacheValue value = new CacheValue(flags, bytes, data);
                        slabCache.set(key, value);
                        testMap.put(key, value);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            futureList.add(service.submit(task));
        }

        // Block until all futures are done.
        for (Future<?> f : futureList) {
            f.get();
        }

        // Verify that all keys are present and nothing should be recycled.
        for (int i=0; i<chunkCount; i++) {
            String key = "test-" + i;
            CacheValue cachedValue = slabCache.get(key);
            CacheValue expectedValue = testMap.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue + " expectedValue: " +
                        expectedValue);
            }
        }
    }
    
    /**
     * Test for parallel get and set calls.
     * Test method for {@link server.cache.SlabCache#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testGetSetParallel()
            throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();

        final int slotsPerPage = PageManager.getPageSize() / slotSize;
        final int chunkCount = slotsPerPage * pageCount * 10;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());

        /*
         * Randomly select a key and query in the cache. If the key exists,
         * then ensure its value is as expected.
         */
        Runnable getTask = new Runnable() {
            @Override
            public void run() {
                try {
                    final String key = "test-" +
                ThreadLocalRandom.current().nextInt(chunkCount);
                    CacheValue cachedValue = slabCache.get(key);
                    if (cachedValue != null) {
                        CacheValue expectedValue = testMap.get(key);
                        if (!expectedValue.equals(cachedValue)) {
                            fail("cachedValue: " + cachedValue +
                                    " expectedValue: " + expectedValue);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        
        final int bytes = 10;
        for (int i = 0; i < chunkCount; i++) {
            final String key = "test-" + i;
            final short flags = (short) i;
            Runnable setTask = new Runnable() {
                @Override
                public void run() {
                    try {
                        final byte[] data = RandomUtils.nextBytes(bytes);

                        CacheValue value = new CacheValue(flags, bytes, data);
                        slabCache.set(key, value);
                        testMap.put(key, value);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            futureList.add(service.submit(setTask));
            futureList.add(service.submit(getTask));
        }

        // Block until all futures are done.
        for (Future<?> f : futureList) {
            f.get();
        }
    }
}
