/**
 * @author Dilip Simha
 */
package server.commands;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Service;

import server.CacheManager;
import server.ConnectionManager;

/**
 * TODO: Move set tests from Connectionmanager tests
 */
public class CommandGetTest {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private CacheManager cacheManager;

    private Service connectionManager;

    private static final String serverIp =
            ConnectionManager.getServerAddress();

    private static final int serverPort = ConnectionManager.getServerPort();

    // Generic charset for all commands. For the keys, we will use custom
    // charset depending on each test case.
//    protected final Charset charset = Charset.forName("ISO-8859-1");
//  protected final Charset charset = Charset.forName("UTF-8");
    protected final Charset charset = Charset.forName("ASCII");

    @Before
    public void setUp() throws IOException {
        Level level = Level.ALL;
        for(Handler h : java.util.logging.Logger.getLogger("").getHandlers()) {
            h.setLevel(level);
        }

        cacheManager = CacheManager.getInstance();
        connectionManager = ConnectionManager.getInstance(cacheManager);
    }

    @After
    public void tearDown() {
        if (connectionManager != null && connectionManager.isRunning()) {
            connectionManager.stopAsync().awaitTerminated();
        }
    }
}
