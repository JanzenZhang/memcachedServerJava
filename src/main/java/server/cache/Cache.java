/**
 * @author Dilip Simha
 */
package server.cache;

/** Generic cache interface */
public interface Cache {
    /**
     * Fetch the cache value.
     *
     * @param key cache key
     * @return cache value if exists; null otherwise.
     */
    CacheValue get(String key) throws InterruptedException;

    /**
     * Set the given value for the given key in cache.
     *
     * @param key cache key
     * @param value cache value.
     * @return true if stored in cache; false otherwise.
     */
    boolean set(String key, CacheValue value) throws InterruptedException;
}
