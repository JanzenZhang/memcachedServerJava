/**
 * @author Dilip Simha
 */
package server;

import java.io.IOException;
import java.util.logging.Logger;

import com.google.common.util.concurrent.Service;

/**
 * Main entry point to this Memcached product.
 * Kicks starts all the necessary services on startup and
 * ensures graceful shutdown upon termination.
 */
public final class Server {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private static Server instance;

    private CacheManager cacheManager;

    private Service connectionManager;

    private Server() throws IOException {
        cacheManager = CacheManager.getInstance();
        connectionManager = ConnectionManager.getInstance(cacheManager);
        LOGGER.finest("Memcached server singleton instance created");
    }

    public static Server getInstance() throws IOException {
        if (instance == null) {
            instance = new Server();
        }
        return instance;
    }

    public void init() {
        // Install shutdown hook
        final Runnable shutdownRunnable = new Runnable() {
            @Override
            public void run() {
                connectionManager.stopAsync().awaitTerminated();
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownRunnable));

        connectionManager.startAsync().awaitRunning();
    }

    public static void main(final String[] args) throws IOException {
        LOGGER.finer("Memcached server starting ...");
        Server server = getInstance();
        server.init();
        LOGGER.finer("Memcached server started");
    }
}
