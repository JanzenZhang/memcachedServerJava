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
public class CommandGet extends AbstractCommand {
    private final static Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    public CommandGet(final CacheManager cacheManager,
            SocketChannel socketChannel) {
        super(cacheManager, socketChannel);
    }

    void process(CommandGetRequest request)
            throws IOException, InterruptedException {
        LOGGER.info("processing ...");
        byte[] keyBytes = request.getKey();

        String key = new String(keyBytes, AbstractCommand.charset);
        CacheValue value = cache.get(key);
        if (value != null) {
            LOGGER.info("cache fetched for key: " + key + " value: " +
                    value.getFlag());
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
    void respondToClient(byte[] key, CacheValue value) throws IOException {
        LOGGER.info("responding to client...");
        CharBuffer cbuf;
        final String newLineMarker = "\r\n";
        final String endMarker = "END";
        if (value != null) {
            final String valueMarker = "VALUE ";
            int bytesToSend = valueMarker.length() + key.length + 1 +
                    Short.BYTES + 1 + Integer.BYTES + newLineMarker.length();
            if (value.getData() != null) {
                bytesToSend += value.getBytes() + newLineMarker.length();
            }
            bytesToSend += endMarker.length() + newLineMarker.length();

            ByteBuffer metadata = ByteBuffer.allocate(bytesToSend);

            metadata.put(charset.encode(valueMarker));
            metadata.put(key);
            cbuf = CharBuffer.wrap(" " + value.getFlag() + " " +
                    value.getBytes() + newLineMarker);
            metadata.put(charset.encode(cbuf));
            metadata.put(value.getData());
            metadata.put(charset.encode(newLineMarker));
            metadata.flip();

            LOGGER.info("keyBytes: " + key);
            LOGGER.info("responded to client: " + new String(metadata.array(), AbstractCommand.charset));
            writeToSocket(metadata);
            metadata = null;
        }

        cbuf = CharBuffer.wrap(endMarker + newLineMarker);
        ByteBuffer endMarkBuf = charset.encode(cbuf);
        writeToSocket(endMarkBuf);
        endMarkBuf = null;
        LOGGER.info("Responded to client for key: " +
                new String(key, AbstractCommand.charset));
    }
}
