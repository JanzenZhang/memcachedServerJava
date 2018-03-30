/**
 * @author Dilip Simha
 */
package server;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Service;

/**
 * Tests end-to-end implementation of Memcached.
 * Tests for NIO selectors used in ConnectionManager.
 * Tests for get and set calls for end-to-end state: socket calls handling to
 * cache behavior.
 */
public class ConnectionManagerTest extends AbstractTest {
    private static final Logger LOGGER = LogManager.getLogger(
            ConnectionManagerTest.class);

    private CacheManager cacheManager;

    private Service connectionManager;

    private static final String serverIp =
            ConnectionManager.getServerAddress();

    private static final int serverPort = ConnectionManager.getServerPort();

    // Generic charset for all commands. For the keys, we will use custom
    // charset depending on each test case.
//    protected final Charset charset = Charset.forName("ISO-8859-1");
  protected final Charset charset = Charset.forName("UTF-8");
//    protected final Charset charset = Charset.forName("ASCII");

    @Before
    public void setUp() throws IOException {
        cacheManager = CacheManager.getInstance();
        connectionManager = ConnectionManager.getInstance(cacheManager);
    }

    @After
    public void tearDown() {
        if (connectionManager != null && connectionManager.isRunning()) {
            connectionManager.stopAsync().awaitTerminated();
        }
    }

    /**
     * Test method for {@link server.ConnectionManager#listenToSockets()}.
     */
    @Test
    public final void testServiceRunning() {
        connectionManager.startAsync().awaitRunning();
        assert (connectionManager.isRunning());
        connectionManager.stopAsync().awaitTerminated();
    }

    /**
     * Test method for {@link server.ConnectionManager#listenToSockets()}.
     */
    @Test
    public final void testClientAccept() throws IOException {
        connectionManager.startAsync().awaitRunning();

        SocketChannel socketChannel = SocketChannel.open(
                new InetSocketAddress(serverIp, serverPort));
        InetSocketAddress remoteSocketAddress =
                (InetSocketAddress) socketChannel.getRemoteAddress();
        assert (serverIp.equals(
                remoteSocketAddress.getAddress().getHostAddress()));
        assert (serverPort == remoteSocketAddress.getPort());
        connectionManager.stopAsync().awaitTerminated();
    }

    protected void writeToSocket(ByteBuffer data, SocketChannel socketChannel)
            throws IOException {
        assert(data.position() == 0);
        while (data.hasRemaining()) {
            int n = socketChannel.write(data);
            LOGGER.trace("Wrote bytes: " + n);
        }
        LOGGER.trace("Wrote to server: " + new String(data.array(), charset));
        data.clear();
    }

    /**
     * Read data bytes from channel.
     * 
     * @param bytesToRead bytes to read on this channel. Blocks until these
     *          many bytes are read.
     */
    private byte[] readBytesFromChannel(SocketChannel socketChannel,
            final int bytesToRead) throws IOException {
        assert (bytesToRead != 0);

        int bytesRead;
        int totalBytesRead = 0;
        byte[] dataBytes = new byte[bytesToRead];
        ByteBuffer data = ByteBuffer.wrap(dataBytes);

        // Keep reading data until bytes amount of data is read from server.
        while (true) {
            bytesRead = socketChannel.read(data);
            if (bytesRead == 0) {
                // TODO: Use some threshold time to loop here to limit the
                // client from waiting forever on erroneous servers.
                Thread.yield();
                continue;
            } else if (bytesRead == -1) {
                LOGGER.warn("Server abruptly closed the connection" +
                        " while request was being processed!");
                return null;
            }

            totalBytesRead += bytesRead;
            if (totalBytesRead == bytesToRead) {
                break;
            }
        }
        LOGGER.trace("totalBytesRead: " + totalBytesRead);
        return dataBytes;
    }

    /**
     * Read data bytes from this socket channel until the delimiter is met.
     * The remaining data if any will be processed accordingly.
     *
     * @param delimiterStr Delimiter string to identify end of required data.
     * @return data bytes read from input not containing the delimiters.
     */
    private byte[] readBytesFromChannel(SocketChannel socketChannel,
            String delimiterStr) throws IOException, InterruptedException {
        assert(delimiterStr != null);
        assert(! delimiterStr.isEmpty());

        ByteBuffer resBuffer = ByteBuffer.allocate(512);
        byte[] delimiters = delimiterStr.getBytes(charset);
        int curDelimIndex = 0;
        final int endDelimIndex = delimiters.length;
        ByteBuffer buf = ByteBuffer.allocate(1);

        while (true) {
            int bytesRead = socketChannel.read(buf);
            if (bytesRead == 0 || bytesRead == -1) {
                LOGGER.trace("readBytesFromChannel bytesRead: " + bytesRead);
                break;
            }
            // Enable buf to be read.
            buf.flip();
            byte b = buf.get();
            if (b == delimiters[curDelimIndex]) {
                curDelimIndex++;
                if (curDelimIndex == endDelimIndex) {
                    // Done reading teh required data
                    break;
                }
            } else if (curDelimIndex != 0) {
                // Input has partial delimiter and its disallowed by protocol.
                LOGGER.debug("Invalid server input. Has control characters");
                fail("Input cannot have control characters: " +
                    new String(resBuffer.array()));
            } else {
                // Just the regular data.
                resBuffer.put(b);
            }
            // Enable buf to be written again.
            buf.flip();
        }
       return Arrays.copyOf(resBuffer.array(),
                resBuffer.position());
    }

    /**
     * Calls cache.set and return the value set in cache.
     * set <key> <flags> <exptime> <bytes> [noreply]\r\n
     * <data block>\r\n
     */
    private byte[] helperSetRequest(final SocketChannel socketChannel,
            final byte[] keyBytes, final int valueBytes, short flags,
            long expTime) throws IOException {
        final String newLineMarker = "\r\n";
        final String setMarker = "set ";

        int bytesToSend = setMarker.length() + keyBytes.length + 1 +
                Short.BYTES + 1 + Long.BYTES + 1 + Integer.BYTES +
                newLineMarker.length();
        if (valueBytes != 0) {
            bytesToSend += valueBytes + newLineMarker.length();
        }

        ByteBuffer byteBufToSend = ByteBuffer.allocate(bytesToSend);

        byte[] data = null;
        if (valueBytes != 0) {
            data = RandomUtils.nextBytes(valueBytes);
        }

        byteBufToSend.put(charset.encode(setMarker));
        byteBufToSend.put(keyBytes);

        CharBuffer charBuf = CharBuffer.wrap(" " + flags + " " + expTime +
                " " + valueBytes + newLineMarker);
        byteBufToSend.put(charset.encode(charBuf));

        if (valueBytes != 0) {
            byteBufToSend.put(data);
            byteBufToSend.put(charset.encode(newLineMarker));
        }

        byteBufToSend.flip();
        writeToSocket(byteBufToSend, socketChannel);
        return data;
    }

    /**
     * Test method for {@link server.ConnectionManager#listenToSockets()}.
     */
    @Test
    public final void testClientSetOp()
            throws IOException, InterruptedException {
        connectionManager.startAsync().awaitRunning();

        SocketChannel socketChannel = SocketChannel.open(
                new InetSocketAddress(serverIp, serverPort));
//      set <key> <flags> <exptime> <bytes> [noreply]\r\n
//      <data block>\r\n

        String keyStr = "123";
        byte[] keyBytes = keyStr.getBytes(charset);
        LOGGER.trace("key bytes : " + keyBytes + " length: " + keyBytes.length);
        helperSetRequest(socketChannel, keyBytes, /*valueBytes=*/ 12,
                /*flags=*/ (short)1, /*expTime=*/ 0);
//        helperSetRequest2(socketChannel, keyStr, /*bytes=*/ 12,
//                /*flags=*/ (short)1, /*expTime=*/ 0);

        // Response format: STORED\r\n OR NOTSTORED\r\n
        byte[] respBuf = readBytesFromChannel(socketChannel, "\r\n");
        if (respBuf == null || respBuf.length == 0) {
            fail("Invalid response");
        }

        final String commandResponse = new String(respBuf, charset);
        LOGGER.trace("commandResponse: " + commandResponse);

        assert(commandResponse.equals("STORED"));
    }

    /**
     * Test method for {@link server.ConnectionManager#listenToSockets()}.
     */
    @Test
    public final void testClientSetOpEmptyData()
            throws IOException, InterruptedException {
        connectionManager.startAsync().awaitRunning();

        SocketChannel socketChannel = SocketChannel.open(
                new InetSocketAddress(serverIp, serverPort));
//      set <key> <flags> <exptime> <bytes> [noreply]\r\n
//      <data block>\r\n

        String keyStr = "1002";
        byte[] keyBytes = keyStr.getBytes(charset);
        helperSetRequest(socketChannel, keyBytes, /*bytes=*/ 0,
                /*flags=*/ (short)1, /*expTime=*/ 0);

        // Response format: STORED\r\n OR NOTSTORED\r\n
        byte[] respBuf = readBytesFromChannel(socketChannel, "\r\n");
        if (respBuf == null || respBuf.length == 0) {
            fail("Invalid response");
        }

        String commandResponse = new String(respBuf, charset);
        LOGGER.trace("commandResponse: " + commandResponse);
        assert(commandResponse.equals("STORED"));
    }

    /**
     * get <key>\r\n
     * Test method for {@link server.ConnectionManager#listenToSockets()}.
     */
    @Test
    public final void testClientGetOpNotFound()
            throws IOException, InterruptedException {
        connectionManager.startAsync().awaitRunning();

        SocketChannel socketChannel = SocketChannel.open(
                new InetSocketAddress(serverIp, serverPort));
        String keyStr = "126";
        byte[] keyBytes = keyStr.getBytes(charset);

        final String getMarker = "get ";
        final String newLineMarker = "\r\n";

        int bytesToSend = getMarker.length() + keyBytes.length +
                newLineMarker.length();
        ByteBuffer byteBufToSend = ByteBuffer.allocate(bytesToSend);

        byteBufToSend.put(charset.encode(getMarker));
        byteBufToSend.put(keyBytes);
        byteBufToSend.put(charset.encode(newLineMarker));
        byteBufToSend.flip();

        LOGGER.trace("Writing get metadata to server: " +
                new String(byteBufToSend.array()));
        writeToSocket(byteBufToSend, socketChannel);

        // Expected Response format: END\r\n
        byte[] respBuf = readBytesFromChannel(socketChannel, "\r\n");
        if (respBuf == null || respBuf.length == 0) {
            fail("Invalid response from server get");
        }

        String commandResponse = new String(respBuf, charset);
        assert(commandResponse.equals("END"));
    }

    /**
     * get <key>\r\n
     * Test method for {@link server.ConnectionManager#listenToSockets()}.
     */
    @Test
    public final void testClientGetOpFound()
            throws IOException, InterruptedException {
        connectionManager.startAsync().awaitRunning();

        SocketChannel socketChannel = SocketChannel.open(
                new InetSocketAddress(serverIp, serverPort));

        // Choose a different charset than the one server uses to read key.
        // This test ensures that charset of key is immaterial to server.
        final Charset keyCharset = Charset.forName("ISO-8859-1");
        final String keyStr = "123";
        final byte[] keyBytes = keyStr.getBytes(keyCharset);
        final int valueBytes = 100;
        final short flags = 1;
        LOGGER.trace("set key: " + keyBytes);
        final byte[] cachedValue = helperSetRequest(socketChannel, keyBytes,
                valueBytes, flags, /*expTime=*/ 0);

        // Response format: STORED\r\n OR NOTSTORED\r\n
        byte[] respBuf = readBytesFromChannel(socketChannel, "\r\n");
        if (respBuf == null || respBuf.length == 0) {
            fail("Invalid response from server for set command");
        }

        String commandResponse = new String(respBuf, charset);
        LOGGER.trace("commandResponse: " + commandResponse);
        assert(commandResponse.equals("STORED"));

        // Now issue a get for this set to ensure it's indeed cached.
        // get <key>\r\n
        final String getMarker = "get ";
        final String newLineMarker = "\r\n";

        int bytesToSend = getMarker.length() + keyBytes.length +
                newLineMarker.length();
        ByteBuffer byteBufToSend = ByteBuffer.allocate(bytesToSend);

        byteBufToSend.put(charset.encode(getMarker));
        byteBufToSend.put(keyBytes);
        byteBufToSend.put(charset.encode(newLineMarker));
        byteBufToSend.flip();

        LOGGER.trace("Writing get command to server: " +
                new String(byteBufToSend.array()));
        LOGGER.trace("get key: " + keyBytes);
        writeToSocket(byteBufToSend, socketChannel);

        // Response format can be either :
        //    VALUE <key> <flags> <bytes>\r\n
        //    <data block>\r\n
        //    END\r\n
        // OR
        //    END\r\n
        byte[] readBytes = readBytesFromChannel(socketChannel, 5);
        if (readBytes == null || readBytes.length == 0) {
            fail("Invalid response from server on get command");
        }
        String readBytesStr = new String(readBytes, charset);
        LOGGER.trace("command first 5 bytes: " + readBytesStr);
        if (readBytesStr.equals("END\r\n")) {
            LOGGER.error("Client received END. Cache element not found!");
            fail("Expecting a cache hit, but we have a miss here");
        } else if (readBytesStr.equals("VALUE")) {
            // Ignore whitespace after VALUE
            readBytesFromChannel(socketChannel, 1);
        }

        final byte[] storedKeyBytes = readBytesFromChannel(socketChannel,
                /*delimiterStr=*/ " ");
        String storedKeyStr = new String(storedKeyBytes, charset);
        LOGGER.trace("received key in get command: " + storedKeyStr);
        LOGGER.trace("stored Key: " + storedKeyStr + " expected key: " + keyStr);
        LOGGER.trace("stored Key bytes: " + storedKeyBytes + " expected key bytes: " + keyBytes);
        LOGGER.trace("stored Key bytes len: " + storedKeyBytes.length +
                " expected key bytes len: " + keyBytes.length);
       assert(storedKeyStr.equals(keyStr));

        readBytes = readBytesFromChannel(socketChannel, "\r\n");
        if (readBytes == null || readBytes.length == 0) {
            fail("Invalid response from server on " +
                    " get command(flags and bytes)");
        }
        readBytesStr = new String(readBytes, charset);
        String[] subParts = readBytesStr.split(" ");
        if (subParts.length != 2) {
            fail("Invalid response from server on get command: subParts" +
                    " length:" + subParts.length);
        }

        final short storedFlags = Short.decode(subParts[0]);
        LOGGER.trace("received flags in get command:: " + storedFlags);
        assert(storedFlags == flags);

        final int storedBytes = Integer.parseInt(subParts[1]);
        LOGGER.trace("received bytes in get command:: " + storedBytes);
        assert(storedBytes == valueBytes);

        if (storedBytes != 0) {
            assert(cachedValue != null);
            byte[] storedValue = readBytesFromChannel(socketChannel,
                    storedBytes);
            assert(Arrays.equals(cachedValue, storedValue));
            
            // expecting: \r\n
            readBytesFromChannel(socketChannel, 2);
        }

        // Expecting: "END\r\n"
        byte[] endMarker = readBytesFromChannel(socketChannel, /*bytes=*/ 5);
        if (endMarker == null || endMarker.length == 0) {
            fail("Invalid endMarker on get command response from server");
        }
        String endMarkerStr = new String(endMarker, charset);

        LOGGER.trace("endMarker: " + endMarkerStr);
        assert(endMarkerStr.equals("END\r\n"));
    }

//    test for multiple get on same channel.

//    test for parallel set and get on same channel.

//    test for parallel set and get on multiple channels.
}
