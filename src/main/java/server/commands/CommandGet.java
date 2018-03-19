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
 * Process the get command end-to-end.
 * Upon fetching the necessary item from cache, write it out to client socket.
 */
public final class CommandGet extends AbstractCommand {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    public CommandGet(final CacheManager cacheManager,
            final SocketChannel socketChannel) {
        super(cacheManager, socketChannel);
    }

    void process(final CommandGetRequest request)
            throws IOException, InterruptedException {
        LOGGER.info("processing ...");
        byte[] keyBytes = request.getKey();

        String key = new String(keyBytes, AbstractCommand.CHARSET);
        CacheValue value = getCache().get(key);
        if (value != null) {
            LOGGER.info("cache fetched for key: " + key + " value: "
                    + value.getFlag());
        } else {
            LOGGER.info("cache cannot fetch for key: " + key);
        }
        respondToClient(keyBytes, value);
    }

    /**
     * Return response to client. Response format:
     * VALUE <key> <flags> <bytes>\r\n
     * <data block>\r\n
     * END\r\n
     */
    void respondToClient(final byte[] key, final CacheValue value)
            throws IOException {
        LOGGER.info("responding to client...");
        CharBuffer cbuf;
        final String newLineMarker = "\r\n";
        final String endMarker = "END";

        if (value != null) {
            final String valueMarker = "VALUE ";
            int bytesToSend = valueMarker.length() + key.length + 1
                    + Short.BYTES + 1 + Integer.BYTES + newLineMarker.length();
            if (value.getData() != null) {
                bytesToSend += value.getBytes() + newLineMarker.length();
            }
            bytesToSend += endMarker.length() + newLineMarker.length();

            ByteBuffer metadata = ByteBuffer.allocate(bytesToSend);

            metadata.put(CHARSET.encode(valueMarker));
            metadata.put(key);
            cbuf = CharBuffer.wrap(" " + value.getFlag() + " "
                    + value.getBytes() + newLineMarker);
            metadata.put(CHARSET.encode(cbuf));
            metadata.put(value.getData());
            metadata.put(CHARSET.encode(newLineMarker));
            metadata.flip();

            LOGGER.info("keyBytes: " + key);
            LOGGER.info("responded to client: "
                    + new String(metadata.array(), AbstractCommand.CHARSET));
            writeToSocket(metadata);
            metadata = null;
        }

        cbuf = CharBuffer.wrap(endMarker + newLineMarker);
        ByteBuffer endMarkBuf = CHARSET.encode(cbuf);
        writeToSocket(endMarkBuf);
        endMarkBuf = null;
        LOGGER.info("Responded to client for key: "
                + new String(key, AbstractCommand.CHARSET));
    }
}
