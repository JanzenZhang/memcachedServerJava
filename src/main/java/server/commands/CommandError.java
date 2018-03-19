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

public final class CommandError extends AbstractCommand {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    public CommandError(final CacheManager cacheManager,
            final SocketChannel socketChannel) {
        super(cacheManager, socketChannel);
    }

    public void respondToClient(final String status, final String errorMsg)
            throws IOException {
        assert (status != null);
        //errorMsg can be null for status=ERROR

        LOGGER.info("responding error message to client...");

        CharBuffer charBuf = null;
        if (status.equals("ERROR")) {
            charBuf = CharBuffer.wrap(status + "\r\n");
        } else if (status.equals("CLIENT_ERROR")) {
            charBuf = CharBuffer.wrap(status + " " + errorMsg + "\r\n");
        } else if (status.equals("SERVER_ERROR")) {
            charBuf = CharBuffer.wrap(status + " " + errorMsg + "\r\n");
        } else {
            LOGGER.severe("Invalid status: " + status);
            assert (false);
        }

        ByteBuffer byteBuf = CHARSET.encode(charBuf);
        writeToSocket(byteBuf);
        byteBuf = null;
    }
}
