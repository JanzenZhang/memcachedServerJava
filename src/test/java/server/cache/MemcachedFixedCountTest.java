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
import org.junit.Test;

import server.AbstractTest;

/**
 */
public class MemcachedFixedCountTest extends AbstractTest {
    MemcachedFixedCount cache;

    /**
     * Test method for {@link server.cache.MemcachedFixedCount#get(java.lang.String)}.
     */
    @Test
    public final void testGetExists() {
        cache = MemcachedFixedCount.getInstance(/*maxCacheSize=*/ 50);

        HashMap<String, CacheValue> testMap = new HashMap<>();

        for (int i = 1; i <= 10; i++) {
            String key = "hello-" + i;
            final short flags = (short) i;
            // Account for data + flags + bytes storage in a CacheSlot.
            final int bytes = RandomUtils.nextInt(/*startInclusive=*/ 0,
                    /*endExclusive=*/ 1024);
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue expectedValue = new CacheValue(flags, bytes, data);
            boolean isStored = cache.set(key, expectedValue);
            assert (isStored);
            testMap.put(key, expectedValue);

            CacheValue cachedValue = cache.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue
                        + " expectedValue: " + expectedValue);
            }
        }
    }

    /**
     * Test method for {@link server.cache.MemcachedFixedCount#get(java.lang.String)}.
     */
    @Test
    public final void testGetNotExists() throws InterruptedException {
        cache = MemcachedFixedCount.getInstance(/*maxCacheSize=*/ 3);

        String key = "hello";
        final short flags = 123;
        final int bytes = 10;
        final byte[] data = RandomUtils.nextBytes(bytes);

        CacheValue value = new CacheValue(flags, bytes, data);
        boolean isStored = cache.set(key, value);
        assert (isStored);

        CacheValue dummyValue = cache.get(key + "-dummy");
        assert (dummyValue == null);
    }
    

    private static String generateRandomKey(ArrayList<String> keys) {
        int randomElementIndex = ThreadLocalRandom.current().nextInt(
                keys.size());
        return keys.get(randomElementIndex);
    }

    /**
     * Test method for {@link server.cache.MemcachedFixedCount#get(java.lang.String)}.
     */
    @Test
    public final void testGetParallel()
            throws InterruptedException, ExecutionException {
        cache = MemcachedFixedCount.getInstance(/*maxCacheSize=*/ 50);

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
            final int maxSizedChunk = 1024;
            final int bytes = RandomUtils.nextInt(/*startInclusive*/ 1,
                    /*endExclusive*/ maxSizedChunk+1);
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue value = new CacheValue(flags, bytes, data);
            boolean isStored = cache.set(key, value);
            assert (isStored);
            testMap.put(key, value);
        }

        final ArrayList<String> keys = new ArrayList<String>(testMap.keySet());

        Runnable task = new Runnable() {
            @Override
            public void run() {
                final String key = generateRandomKey(keys);
                final CacheValue cachedValue = cache.get(key);
                final CacheValue expectedValue = testMap.get(key);
                if (!expectedValue.equals(cachedValue)) {
                    fail("key: " + key + " cachedValue: " + cachedValue +
                            " expectedValue: " + expectedValue.getFlag());
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
     * Test method for {@link server.cache.MemcachedFixedCount#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testSetParallel()
            throws InterruptedException, ExecutionException {
        cache = MemcachedFixedCount.getInstance(/*maxCacheSize=*/ 50);

        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        // Ensure chunk count is not greater than
        // maxCacheSize/largest sized element.
        final int chunkCount = 20;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());

        final int maxSizedChunk = 1024;
        final int bytes = RandomUtils.nextInt(/*startInclusive*/ 1,
                /*endExclusive*/ maxSizedChunk+1);

        for (int i = 0; i < chunkCount; i++) {
            final String key = "test-" + i;
            final short flags = (short) i;
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    final byte[] data = RandomUtils.nextBytes(bytes);
                    CacheValue value = new CacheValue(flags, bytes, data);
                    boolean isStored = cache.set(key, value);
                    assert (isStored);
                    testMap.put(key, value);
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
     * Test method for {@link server.cache.MemcachedFixedCount#set(java.lang.String, server.cache.CacheValue)}.
     */
    @Test
    public final void testGetSetParallel()
            throws InterruptedException, ExecutionException {
        cache = MemcachedFixedCount.getInstance(/*maxCacheSize=*/ 50);

        ExecutorService service = Executors.newFixedThreadPool(3);
        List<Future<?>> futureList = new LinkedList<>();
        final int chunkCount = 30;
        Map<String, CacheValue> testMap = Collections.synchronizedMap(
                new HashMap<>());
        final int maxSizedChunk = 1024;

        /*
         * Randomly select a key and query in the cache. If the key exists,
         * then ensure its value is as expected.
         */
        Runnable getTask = new Runnable() {
            @Override
            public void run() {
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
                    final byte[] data = RandomUtils.nextBytes(bytes);
                    CacheValue value = new CacheValue(flags, bytes, data);
                    cache.set(key, value);
                    testMap.put(key, value);
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

    /**
     * Test method for {@link server.cache.MemcachedFixedCount}.
     */
    @Test
    public final void testEviction() {
        final long maxCacheSize = 50;
        cache = MemcachedFixedCount.getInstance(maxCacheSize);

        HashMap<String, CacheValue> testMap = new HashMap<>();
        final int chunksCountUptoEviction = (int) maxCacheSize;

        for (int i = 1; i <= chunksCountUptoEviction; i++) {
            String key = "hello-" + i;
            final short flags = (short) i;
            final int bytes = RandomUtils.nextInt(/*startInclusive=*/ 0,
                    /*endExclusive=*/ 1024);
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue expectedValue = new CacheValue(flags, bytes, data);
            boolean isStored = cache.set(key, expectedValue);
            assert (isStored);
            testMap.put(key, expectedValue);

            CacheValue cachedValue = cache.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue
                        + " expectedValue: " + expectedValue);
            }
        }

        // Verify that none of the entries inserted so far are evicted yet.
        for (int i = 1; i <= chunksCountUptoEviction; i++) {
            String key = "hello-" + i;

            CacheValue expectedValue = testMap.get(key);
            CacheValue cachedValue = cache.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue
                        + " expectedValue: " + expectedValue);
            }
        }

        // Now fore very new entry, oldest element in cache should be evicted.
        // Ensure that evicted elements are as expected.
        int expectedEvictedKeySuffix = 1;
        for (int i = chunksCountUptoEviction; i<=chunksCountUptoEviction*2;
                i++) {
            String key = "hello-" + i;
            final short flags = (short) i;
            final int bytes = RandomUtils.nextInt(/*startInclusive=*/ 0,
                    /*endExclusive=*/ 1024);
            final byte[] data = RandomUtils.nextBytes(bytes);

            CacheValue expectedValue = new CacheValue(flags, bytes, data);
            boolean isStored = cache.set(key, expectedValue);
            assert (isStored);
            testMap.put(key, expectedValue);

            CacheValue cachedValue = cache.get(key);
            if (!expectedValue.equals(cachedValue)) {
                fail("cachedValue: " + cachedValue
                        + " expectedValue: " + expectedValue);
            }

            final String expectedEvictedKey = "hello" +
                    expectedEvictedKeySuffix;

            cachedValue = cache.get(expectedEvictedKey);
            if (cachedValue != null) {
                fail("Expected eviction on key: " + expectedEvictedKey
                        + " cachedValue: " + cachedValue);
            }

            // Eviction is strict LRU.
            expectedEvictedKeySuffix++;
        }
    }
}
