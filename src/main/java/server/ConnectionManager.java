/**
 * @author Dilip Simha
 */
package server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.AbstractIdleService;

import server.commands.CommandProcessor;

/**
 *  Connection Manager is a service to manage connections. Its a singleton.
 *  It uses NIO:selector to manage all new incoming connections as well as
 *  follow-up commands from clients.
 *  Processing a specific request is handled by worker threads which are
 *  managed by a ThreadPoolExecutor.
 *  Upon processing a command, the worker thread replies to client on the
 *  same connection.
 *  Any issues with a particular client request will be logged accordingly
 *  and reported to client as CLIENT error or server error as per the protocol
 *  mandate.
 */
public final class ConnectionManager extends AbstractIdleService {
    private static final Logger LOGGER = LogManager.getLogger(
            ConnectionManager.class);

    private static ConnectionManager instance;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private CacheManager cacheManager;

    private ServerSocketChannel serverSocket;

    private Selector selector;

    private final int coreWorkerPoolSize;

    private final int maximumWorkerPoolSize;

    private static final long KEEP_ALIVE_TIME = 10; // 10 mins

    private final ThreadPoolExecutor workerThreads;

    private static final int MAX_CONCURRENT_REQUESTS = 1024;

    private final BlockingQueue<Runnable> workerTasksQueue;

    private static final String SERVER_ADDRESS = "0.0.0.0";

    private static final int SERVER_PORT = 11211;

    private static final int SELECTOR_TIMEOUT_MS = 2;

    private ConnectionManager(final CacheManager cacheMgr)
            throws IOException {
        this.cacheManager = cacheMgr;

        selector = Selector.open();
        workerTasksQueue = new ArrayBlockingQueue<>(
                MAX_CONCURRENT_REQUESTS);
        maximumWorkerPoolSize = Runtime.getRuntime().availableProcessors();
        LOGGER.trace("maximumWorkerPoolSize: " + maximumWorkerPoolSize);
        coreWorkerPoolSize = Math.max(1, maximumWorkerPoolSize / 2);
        workerThreads = new ThreadPoolExecutor(coreWorkerPoolSize,
                maximumWorkerPoolSize, KEEP_ALIVE_TIME, TimeUnit.MINUTES,
                workerTasksQueue);

        configureServerSocket();
        LOGGER.trace("ConnectionManager singleton instance created");
    }

    public static ConnectionManager getInstance(
            final CacheManager cacheManager) throws IOException {
        if (instance == null) {
            instance = new ConnectionManager(cacheManager);
        }
        assert (instance.cacheManager == cacheManager);
        return instance;
    }

    public static String getServerAddress() {
        return SERVER_ADDRESS;
    }

    public static int getServerPort() {
        return SERVER_PORT;
    }

    private void configureServerSocket() throws IOException {
        serverSocket = ServerSocketChannel.open();
        // If the server shutdown and restarts immediately, then tell the
        // kernel to reuse this address even though its state is in TIME_WAIT.
        serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverSocket.bind(new InetSocketAddress(SERVER_ADDRESS, SERVER_PORT));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void handleNewConnections(final SelectionKey key)
            throws IOException {
        ServerSocketChannel serverSocketChannel =
                (ServerSocketChannel) key.channel();
        SocketChannel client = serverSocketChannel.accept();
        assert (client != null);
        LOGGER.trace("Accepted client request: "
                + client.socket().getLocalAddress() + " : "
                + client.socket().getLocalPort());
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    private void handleReadRequest(final SelectionKey key) {
        LOGGER.trace("Thread: "
                + Thread.currentThread().getId()
                + " : client request for READ: ");
        // Until data on this channel is consumed, do not
        // fire any more read events.
        key.interestOps(0);
        SocketChannel socketChannel =
                (SocketChannel) key.channel();
        workerThreads.execute(new CommandProcessor(
                cacheManager, socketChannel, key));
    }

    public void listenToSockets() {
        LOGGER.info("ConnectionManager listening ...");
        while (true) {
            try {
                // Blocking call
                int ready = selector.select(SELECTOR_TIMEOUT_MS);
                if (!selector.isOpen()) {
                    // Shutdown closed the selector and hence stop this
                    // infinite loop.
                    LOGGER.debug("listenToSockets: selector closed");
                    break;
                }
                if (ready != 0) {
                    // Protect selector.keys() using selector as the key set
                    // is not thread-safe.
                    synchronized (selector) {
                        Set<SelectionKey> keys = selector.selectedKeys();
                        if (keys == null) {
                            continue;
                        }
                        Iterator<SelectionKey> keyIterator = keys.iterator();
                        while (keyIterator.hasNext()) {
                            SelectionKey key = keyIterator.next();
                            if (key.isAcceptable()) {
                                handleNewConnections(key);
                            } else if (key.isReadable()) {
                                handleReadRequest(key);
                            } else if (key.isWritable()) {
                                LOGGER.error("client request for WRITE"
                                        + " not yet supported.");
                                assert (false);
                            } else {
                                LOGGER.error("client request for invalid"
                                        + " type: " + key.readyOps());
                                assert (false);
                            }
                            keyIterator.remove();
                        }
                        keys.clear();
                    }
                } else {
                    Thread.yield();
                }
            } catch (AsynchronousCloseException e) {
                LOGGER.trace("Multiple test threads working on same channel."
                        + " Safe to stop here.");
                break;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClosedSelectorException e) {
                // Shutdown closed this selector. Gracefully break out.
                break;
            }
        }
        LOGGER.info("ConnectionManager stopped listening");
    }

    @Override
    protected void startUp() {
        LOGGER.trace("Connection Manager starting ...");
        executor.execute(new Runnable() {
            @Override
            public void run() {
                listenToSockets();
            }
        });
        LOGGER.trace("Connection Manager started");
    }

    /**
     * Shutdown the ConnectionManager service gracefully.
     * First close the selector, stopping new connections.
     * Then cancel the already registered channels from the selector.
     * Then wait for worker threads that are currently handling requests.
     * Finally, close all channels and terminate this service.
     */
    @Override
    protected void shutDown() throws InterruptedException, IOException {
        LOGGER.debug("Connection Manager shutting down ...");
        // First close the server socket so that new connections are not made.
        if (serverSocket != null) {
            serverSocket.close();
        }
        List<SocketChannel> channelsToRemove = new LinkedList<>();

        // Protect selector.keys() using selector as the key set is not
        // thread-safe.
        synchronized (selector) {
            Set<SelectionKey> keys = selector.selectedKeys();
            if (keys != null) {
                Iterator<SelectionKey> keyIterator = keys.iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    SelectableChannel channel = key.channel();
                    if (channel instanceof SocketChannel) {
                        SocketChannel socketChannel = (SocketChannel) channel;
                        channelsToRemove.add(socketChannel);
                        key.cancel();
                    } else {
                        key.cancel();
                    }
                }
            }
        }
        selector.close();

        workerThreads.shutdown();
        if (!workerThreads.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warn("WorkerThreads cannot be shut down within"
                    + " timeout: 1 min");
        }

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warn("Executor cannot be shut down within"
                    + " timeout: 1 min");
        }

        // Now close connections to all clients.
        for (SocketChannel client : channelsToRemove) {
            Socket socket = client.socket();
            String remoteHost = socket.getRemoteSocketAddress().toString();
            LOGGER.trace("closing socket: " + remoteHost);
            client.close();
        }
        channelsToRemove.clear();

        LOGGER.info("Connection Manager shut down");
    }
}
