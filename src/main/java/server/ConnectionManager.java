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
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

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
        LOGGER.finest("maximumWorkerPoolSize: " + maximumWorkerPoolSize);
        coreWorkerPoolSize = Math.max(1, maximumWorkerPoolSize / 2);
        workerThreads = new ThreadPoolExecutor(coreWorkerPoolSize,
                maximumWorkerPoolSize, KEEP_ALIVE_TIME, TimeUnit.MINUTES,
                workerTasksQueue);

        configureServerSocket();
        LOGGER.finest("ConnectionManager singleton instance created");
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

    public void listenToSockets() {
        LOGGER.finest("ConnectionManager listenToSockets:");
        while (true) {
            try {
                // Blocking call
                int ready = selector.select(SELECTOR_TIMEOUT_MS);
                if (ready != 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = keys.iterator();
                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        if (key.isAcceptable()) {
                            ServerSocketChannel serverSocketChannel =
                                    (ServerSocketChannel) key.channel();
                            SocketChannel client =
                                    serverSocketChannel.accept();
                            assert (client != null);
                            LOGGER.finest("Accepted client request: "
                                    + client.socket().getLocalAddress() + " : "
                                    + client.socket().getLocalPort());
                            client.configureBlocking(false);
                            client.register(selector, SelectionKey.OP_READ);
                        } else if (key.isReadable()) {
                            LOGGER.finest("Thread: "
                                    + Thread.currentThread().getId()
                                    + " : client request for READ: ");
                            // Until data on this channel is consumed, do not
                            // fire any more read events.
                            key.interestOps(0);
                            SocketChannel socketChannel =
                                    (SocketChannel) key.channel();
                            workerThreads.execute(new CommandProcessor(
                                    cacheManager, socketChannel, key));
                        } else if (key.isWritable()) {
                            LOGGER.severe("client request for WRITE"
                                    + " not yet supported.");
                            assert (false);
                        } else {
                            LOGGER.severe("client request for invalid type: "
                                    + key.readyOps());
                            assert (false);
                        }
                        keyIterator.remove();
                    }
                    keys.clear();
                } else {
                    Thread.yield();
                }
            } catch (AsynchronousCloseException e) {
                LOGGER.finest("Multiple test threads working on same channel."
                        + " Safe to stop here.");
                break;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClosedSelectorException e) {
                LOGGER.finest("Time to wrap up. Bye bye to listenToSockets");
                break;
            }
        }
    }

    @Override
    protected void startUp() {
        LOGGER.finest("Connection Manager starting ...");
        executor.execute(new Runnable() {
            @Override
            public void run() {
                listenToSockets();
            }
        });
        LOGGER.finest("Connection Manager started");
    }

    @Override
    protected void shutDown() throws InterruptedException, IOException {
        LOGGER.finest("Connection Manager shutting down ...");
        if (serverSocket != null) {
            serverSocket.close();
        }

        Iterator<SelectionKey> keys = this.selector.keys().iterator();
        while (keys.hasNext()) {
            SelectionKey key = keys.next();
            SelectableChannel channel = key.channel();
            if (channel instanceof SocketChannel) {
                SocketChannel socketChannel = (SocketChannel) channel;
                Socket socket = socketChannel.socket();
                String remoteHost = socket.getRemoteSocketAddress().toString();
                LOGGER.finest("closing socket: " + remoteHost);
                socketChannel.close();
                key.cancel();
            } else {
                key.cancel();
            }
        }
        selector.close();

        workerThreads.shutdown();
        if (!workerThreads.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warning("WorkerThreads cannot be shut down within"
                    + " timeout: 1 min");
        }

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warning("Executor cannot be shut down within"
                    + " timeout: 1 min");
        }
        LOGGER.finest("Connection Manager shut down");
    }
}
