/**
 * @author Dilip Simha
 */
package server.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import server.CacheManager;
import server.cache.Cache;

public abstract class AbstractCommand {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    protected Cache cache;
    protected SocketChannel socketChannel;
    static final Charset charset = Charset.forName("UTF-8");

    AbstractCommand(final CacheManager cacheManager,
            final SocketChannel socketChannel) {
        this.cache = cacheManager.getCache();
        this.socketChannel = socketChannel;
    }

    protected void writeToSocket(ByteBuffer data) throws IOException {
//        data.compact();
//        data.flip();
        // Ensure we always have the buffer all set to be read.
        assert(data.position() == 0);
        while (data.hasRemaining()) {
            int n = socketChannel.write(data);
            LOGGER.info("Wrote bytes: " + n);
        }
        data.clear();
    }
}
