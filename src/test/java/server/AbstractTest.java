package server;

import java.lang.reflect.Field;

import org.junit.Before;

import server.cache.MemcachedFixedCount;
import server.cache.MemcachedFixedSizeSlabLRUPages;
import server.cache.PageManager;

public abstract class AbstractTest {
    @Before
    public void setup() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        resetSingleton(PageManager.class);
        resetSingleton(ConnectionManager.class);
        resetSingleton(CacheManager.class);
        resetSingleton(MemcachedFixedSizeSlabLRUPages.class);
        resetSingleton(MemcachedFixedCount.class);
    }

    private static void resetSingleton(
            @SuppressWarnings("rawtypes") Class myClass)
            throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
       Field instance = myClass.getDeclaredField("instance");
       instance.setAccessible(true);
       instance.set(null, null);
    }
}
