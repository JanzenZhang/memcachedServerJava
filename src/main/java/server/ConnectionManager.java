/**
 * @author Dilip Simha
 */
package server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
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
public class ConnectionManager extends AbstractIdleService {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private static ConnectionManager instance;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private CacheManager cacheManager;
    
    private ServerSocketChannel serverSocket;

    private Selector selector;

    private final int coreWorkerPoolSize;

    private final int maximumWorkerPoolSize;

    private static final long keepAliveTime = 10; // 10 mins

    private final ThreadPoolExecutor workerThreads;

    private static final int maxConcurrentRequests = 1024;

    private final BlockingQueue<Runnable> workerTasksQueue;

    private static final String serverAddress = "0.0.0.0";

    private static final int serverPort = 11211;

    private static final int selectorTimeoutms = 2;

    private ConnectionManager(final CacheManager cacheManager)
            throws IOException {
        this.cacheManager = cacheManager;

        selector = Selector.open();
        workerTasksQueue = new ArrayBlockingQueue<>(
                maxConcurrentRequests);
        maximumWorkerPoolSize = Runtime.getRuntime().availableProcessors();
        LOGGER.finest("maximumWorkerPoolSize: " + maximumWorkerPoolSize);
        coreWorkerPoolSize = Math.max(1, maximumWorkerPoolSize/2);
        workerThreads = new ThreadPoolExecutor(coreWorkerPoolSize,
                maximumWorkerPoolSize, keepAliveTime, TimeUnit.MINUTES,
                workerTasksQueue);

        configureServerSocket();
        LOGGER.info("ConnectionManager singleton instance created");
    }

    public static ConnectionManager getInstance(
            final CacheManager cacheManager) throws IOException {
        if (instance == null) {
            instance = new ConnectionManager(cacheManager);
        }
        assert(instance.cacheManager == cacheManager);
        return instance;
    }

    public static String getServerAddress() {
        return serverAddress;
    }

    public static int getServerPort() {
        return serverPort;
    }

    private void configureServerSocket() throws IOException {
        serverSocket = ServerSocketChannel.open();
        // If the server shutdown and restarts immediately, then tell the
        // kernel to reuse this address even though its state is in TIME_WAIT.
        serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverSocket.bind(new InetSocketAddress(serverAddress, serverPort));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void listenToSockets() {
        LOGGER.info("ConnectionManager listenToSockets:");
        while (true) {
            try {
                // Blocking call
                int ready = selector.select(selectorTimeoutms);
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
                            assert(client != null);
                            LOGGER.info("Accepted client request: " +
                                    client.socket().getLocalAddress() + " : " +
                                    client.socket().getLocalPort());
                            client.configureBlocking(false);
                            client.register(selector, SelectionKey.OP_READ);
                        } else if (key.isReadable()) {
                            LOGGER.info("Thread: " + Thread.currentThread().getId() +
                                    " : client request for READ: ");
                            // Until data on this channel is consumed, do not
                            // fire any more read events. 
                            key.interestOps(0);
                            SocketChannel socketChannel =
                                    (SocketChannel) key.channel();
                            workerThreads.execute(new CommandProcessor(
                                    cacheManager, socketChannel, key));
                        } else if (key.isWritable()) {
                            LOGGER.severe("client request for WRITE" +
                                    " not yet supported.");
                            assert (false);
                        } else {
                            LOGGER.severe("client request for invalid type: " +
                                    key.readyOps());
                            assert (false);
                        }
                        keyIterator.remove();
                    }
                    keys.clear();
                } else {
                    Thread.yield();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClosedSelectorException e) {
                LOGGER.info("Time to wrap up. Bye bye to listenToSockets");
                break;
            }
        }
    }

    @Override
    protected void startUp() {
        LOGGER.info("Connection Manager starting ...");
        executor.execute(new Runnable() {
            @Override
            public void run() {
                listenToSockets();
            }
        });
        LOGGER.info("Connection Manager started");
    }

    @Override
    protected void shutDown() throws InterruptedException, IOException {
        LOGGER.info("Connection Manager shutting down ...");
        if (serverSocket != null) {
            serverSocket.close();
        }

        Iterator<SelectionKey> keys = this.selector.keys().iterator();
        while(keys.hasNext()) {
            SelectionKey key = keys.next();
            SelectableChannel channel = key.channel();
            if (channel instanceof SocketChannel) {
                SocketChannel socketChannel = (SocketChannel) channel;
                Socket socket = socketChannel.socket();
                String remoteHost = socket.getRemoteSocketAddress().toString();
                LOGGER.info("closing socket: " + remoteHost);
                socketChannel.close();
                key.cancel();
            } else {
                key.cancel();
            }
        }
        selector.close();

        workerThreads.shutdown();
        if (! workerThreads.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warning("WorkerThreads cannot be shut down within" +
                    " timeout: 1 min");
        }

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warning("Executor cannot be shut down within" +
                    " timeout: 1 min");
        }
        LOGGER.info("Connection Manager shut down");
    }
}
