/**
 * @author Dilip Simha
 */
package server.cache;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
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

import server.AbstractTest;
import server.cache.CacheValue;
import server.cache.MemcachedFixedSizeSlabLRUPages;
import server.cache.PageManager;
import server.cache.SlabCache;

/**
 *
 */
public class MemcachedFixedSizeSlabLRUPagesTest extends AbstractTest {
    private final int pageCount = 10;
    private final long maxGlobalCacheSize =
            pageCount * PageManager.getPageSize();
    MemcachedFixedSizeSlabLRUPages cache;

    @Before
    public void setUp() {
        cache = MemcachedFixedSizeSlabLRUPages.getInstance(maxGlobalCacheSize);
    }

    @After
    public void tearDown() {
    }

    /**
     * Ensure slabcaches are sized appropriately. CacheValues matching the
     * slabCache sizes should make its way into appropriate slabCaches.
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#initializeSlabs()}.
     */
    @Test
    public final void testSlabSizes() throws InterruptedException {
        Collection<SlabCache> slabcaches = cache.getSlabCaches();
        assert (slabcaches != null);
        assert (slabcaches.size() ==
                MemcachedFixedSizeSlabLRUPages.getMaxSlabs());

        for (SlabCache slabcache : slabcaches) {
            assert (slabcache.getCacheSize() == 0);
        }

        for (int i = 4; i <= 22; i += 2) {
            int slotSize = (int) Math.pow(2, i);

            String key = "hello-" + slotSize;
            final short flags = (short) i;
            // Account for data + flags + bytes storage in a CacheSlot.
            final int bytes = slotSize - Short.BYTES - Integer.BYTES;
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue value = new CacheValue(flags, bytes, data);
            cache.set(key, value);
        }

        for (SlabCache slabCache : slabcaches) {
            assert (slabCache.getCacheSize() == 1);
            System.out.println("slabCache: " + slabCache.getSlotSize()
                + " size: " + slabCache.getCacheSize());
        }
    }

    /**
     * Test SlabSize boundaries. Ensure that SlabCaches do not overflow.
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#initializeSlabs()}.
     */
    @Test
    public final void testSlabSizeBoundaries() throws InterruptedException {
        Collection<SlabCache> slabcaches = cache.getSlabCaches();

        for (int i = 4; i <= 20; i += 2) {
            int slotSize = (int) Math.pow(2, i);

            String key = "hello-" + slotSize;
            final short flags = (short) i;
            // this element should go into next higher sized SlabCache
            // because a CacheSlot holds data + flags + bytes fields.
            final int bytes = slotSize;
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue value = new CacheValue(flags, bytes, data);
            cache.set(key, value);
        }

        for (SlabCache slabCache : slabcaches) {
            // Ensure that all slabCaches except the first have exactly one
            // element.
            if (slabCache.getSlotSize() != 16) {
                assert (slabCache.getCacheSize() == 1);
            } else {
                assert (slabCache.getCacheSize() == 0);
            }
            System.out.println("slabCache: " + slabCache.getSlotSize()
                + " size: " + slabCache.getCacheSize());
        }
        
        // Ensure that cache elements sized larger than largest slotSized
        // slabCache is not cached.
        int slotSize = (int) Math.pow(2, 22);
        String key = "hello-" + slotSize;
        final short flags = (short) 22;
        // Deliberately do not account for flags and bytes fields in cacheValue
        final int bytes = slotSize;
        final byte[] data = RandomUtils.nextBytes(bytes);

        CacheValue value = new CacheValue(flags, bytes, data);
        cache.set(key, value);
        
        CacheValue noValue = cache.get(key);
        assert (noValue == null);
    }

    /**
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#get(java.lang.String)}.
     */
    @Test
    public final void testGetExists() throws InterruptedException {
        HashMap<String, CacheValue> testMap = new HashMap<>();

        Collection<SlabCache> slabcaches = cache.getSlabCaches();

        for (int i = 4; i <= 22; i += 2) {
            int slotSize = (int) Math.pow(2, i);

            String key = "hello-" + slotSize;
            final short flags = (short) i;
            // Account for data + flags + bytes storage in a CacheSlot.
            final int bytes = slotSize - Short.BYTES - Integer.BYTES;
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue expectedValue = new CacheValue(flags, bytes, data);
            cache.set(key, expectedValue);
            testMap.put(key, expectedValue);

            CacheValue cachedValue = cache.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue
                        + " expectedValue: " + expectedValue);
            }
        }

        for (SlabCache slabCache : slabcaches) {
            final String key = "hello-" + slabCache.getSlotSize();
            CacheValue cachedValue = slabCache.get(key);
            assert (cachedValue != null);
            CacheValue expectedValue = testMap.get(key);
            assert (expectedValue != null);
            if (! expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue
                        + " expectedValue: " + expectedValue);
            }
        }
    }

    /**
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#get(java.lang.String)}.
     */
    @Test
    public final void testGetNotExists() throws InterruptedException {
        String key = "hello";
        final short flags = 123;
        final int bytes = 10;
        final byte[] data = RandomUtils.nextBytes(bytes);

        CacheValue value = new CacheValue(flags, bytes, data);
        cache.set(key, value);

        CacheValue dummyValue = cache.get(key + "-dummy");
        assert(dummyValue == null);
    }

    private static String generateRandomKey(ArrayList<String> keys) {
        int randomElementIndex = ThreadLocalRandom.current().nextInt(
                keys.size());
        return keys.get(randomElementIndex);
    }

    /**
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#get(java.lang.String)}.
     */
    @Test
    public final void testGetParallel()
            throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        // Ensure chunk count is not greater than
        // maxCacheSize/largest sized element.
        final int chunkCount = 20;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());

        for (int i=0; i<chunkCount; i++) {
            String key = "test-" + i;
            final short flags = (short) i;
            final int maxSizedChunk = (int) Math.pow(2, 22) - Short.BYTES -
                    Integer.BYTES;
            final int bytes = RandomUtils.nextInt(/*startInclusive*/ 1,
                    /*endExclusive*/ maxSizedChunk+1);
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue value = new CacheValue(flags, bytes, data);
            cache.set(key, value);
            testMap.put(key, value);
//            System.out.println("key: " + key + " size: " + value.getSerializedSize());
        }

        final ArrayList<String> keys = new ArrayList<String>(testMap.keySet());

        Runnable task = new Runnable() {
            @Override
            public void run() {
                String key = generateRandomKey(keys);
                try {
                    CacheValue cachedValue = cache.get(key);
                    CacheValue expectedValue = testMap.get(key);
                    if (! expectedValue.equals(cachedValue)) {
                        fail("key: " + key + " cachedValue: " + cachedValue +
                                " expectedValue: " + expectedValue.getFlag());
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
     * Test for parallel set calls.
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testSetParallel()
            throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        // Ensure chunk count is not greater than
        // maxCacheSize/largest sized element.
        final int chunkCount = 20;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());

        final int maxSizedChunk = (int) Math.pow(2, 22) - Short.BYTES -
                Integer.BYTES;
        final int bytes = RandomUtils.nextInt(/*startInclusive*/ 1,
                /*endExclusive*/ maxSizedChunk+1);

        for (int i = 0; i < chunkCount; i++) {
            final String key = "test-" + i;
            final short flags = (short) i;
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        final byte[] data = RandomUtils.nextBytes(bytes);
                        CacheValue value = new CacheValue(flags, bytes, data);
                        cache.set(key, value);
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
            CacheValue cachedValue = cache.get(key);
            CacheValue expectedValue = testMap.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue + " expectedValue: " +
                        expectedValue);
            }
        }
    }

    /**
     * Test for parallel get and set calls.
     * Test method for {@link server.cache.MemcachedFixedSizeSlabLRUPages#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testGetSetParallel()
            throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        final int chunkCount = 50;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());
        final int maxSizedChunk = (int) Math.pow(2, 22) - Short.BYTES -
                Integer.BYTES;

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
                    CacheValue cachedValue = cache.get(key);
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

        final int bytes = RandomUtils.nextInt(/*startInclusive*/ 1,
                /*endExclusive*/ maxSizedChunk+1);
        for (int i = 0; i < chunkCount; i++) {
            final String key = "test-" + i;
            final short flags = (short) i;
            Runnable setTask = new Runnable() {
                @Override
                public void run() {
                    try {
                        final byte[] data = RandomUtils.nextBytes(bytes);
                        CacheValue value = new CacheValue(flags, bytes, data);
                        cache.set(key, value);
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
