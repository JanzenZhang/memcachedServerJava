/**
 * @author Dilip Simha
 */
package server.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import server.CacheManager;
import server.cache.Cache;

public abstract class AbstractCommand {
    private static final Logger LOGGER = LogManager.getLogger(
            AbstractCommand.class);

    private Cache cache;
    private SocketChannel socketChannel;
    static final Charset CHARSET = Charset.forName("UTF-8");

    AbstractCommand(final CacheManager cacheManager,
            final SocketChannel socketChannel) {
        this.cache = cacheManager.getCache();
        this.socketChannel = socketChannel;
    }

    protected final Cache getCache() {
        return cache;
    }

    protected final void writeToSocket(final ByteBuffer data)
            throws IOException {
        // Ensure we always have the buffer all set to be read.
        assert (data.position() == 0);

        while (data.hasRemaining()) {
            int n = socketChannel.write(data);
            LOGGER.trace("Wrote bytes: " + n);
        }
        data.clear();
    }
}
