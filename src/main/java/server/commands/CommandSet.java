/**
 * @author Dilip Simha
 */
package server.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import server.CacheManager;
import server.cache.CacheValue;

/**
 * Process the set command end-to-end.
 * Upon fetching the necessary item from client socket, write it out to cache.
 */
public class CommandSet extends AbstractCommand {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    public CommandSet(final CacheManager cacheManager,
            final SocketChannel socketChannel) {
        super(cacheManager, socketChannel);
    }

    public final void process(final CommandSetRequest request,
            final byte[] data) throws IOException, InterruptedException {
        CacheValue value = new CacheValue(request.flags, request.bytes, data);

        // Store the key in whatever format, it doesn;t matter. But make sure
        // to use the same format consistently in the server.
        String key = new String(request.key, AbstractCommand.CHARSET);

        boolean cached = getCache().set(key, value);
        if (cached) {
            respondToClient(CommandSetResponse.STORED);
        } else {
            respondToClient(CommandSetResponse.NOT_STORED);
        }
        LOGGER.finest("Responded to client for key: " + request.key);
    }

    private void respondToClient(final CommandSetResponse response)
            throws IOException {
        LOGGER.finest("responding to client...");

        CharBuffer charBuf;
        if (response == CommandSetResponse.STORED) {
            charBuf = CharBuffer.wrap("STORED" + "\r\n");
        } else {
            charBuf = CharBuffer.wrap("NOTSTORED" + "\r\n");
        }
        ByteBuffer byteBuf = CHARSET.encode(charBuf);
        writeToSocket(byteBuf);
        byteBuf = null;
    }
}
