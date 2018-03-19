/**
 * @author Dilip Simha
 */
package server.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import server.CacheManager;

/**
 * Identify if the command is "get" or "set" and then hand over control to
 * specific commands. Any issues with a command will be logged accordingly and
 * that corresponding thread dies.
 */
public class CommandProcessor implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(
            Thread.currentThread().getStackTrace()[0].getClassName());

    private CacheManager cacheManager;

    private SocketChannel socketChannel;
    
    private SelectionKey selectionKey;

    public CommandProcessor(final CacheManager cacheManager,
            final SocketChannel socketChannel,
            final SelectionKey selectionKey) {
        this.cacheManager = cacheManager;
        this.socketChannel = socketChannel;
        this.selectionKey = selectionKey;
        // Ensure that this key doesn't fire any more events.
        // That prevents multiple threads from processing data from the same
        // channel.
        assert (selectionKey.interestOps() == 0);
    }

    private boolean validKey(final String key) {
        Pattern p = Pattern.compile("[^a-z0-9]", Pattern.CASE_INSENSITIVE);
        if (p.matcher(key).find()) {
            LOGGER.finest("Invalid Key: " + key
                    + " contains control characters");
            return false;
        } else if (key.contains(" ")) {
            // Should not be the case as our logic uses space as delimiter
            // to extract subparts of the command HEADER.
            LOGGER.finest("Invalid Key: " + key + " contains spaces");
            return false;
        } else if (key.length() > 250) {
            LOGGER.finest("Invalid Key: " + key + " size: " + key.length()
                    + " > 250");
            return false;
        }
        return true;
    }

    /**
     * Read data bytes from channel.
     * @param bytesToRead bytes to read on this channel. Blocks until these
     *          many bytes are read.
     */
    private byte[] readBytesFromChannel(final int bytesToRead)
            throws IOException {
        assert (bytesToRead != 0);

        int bytesRead;
        int totalBytesRead = 0;
        byte[] dataBytes = new byte[bytesToRead];
        ByteBuffer data = ByteBuffer.wrap(dataBytes);

        // Keep reading data until bytes amount of data is read from client.
        while (true) {
            bytesRead = socketChannel.read(data);
            if (bytesRead == 0) {
                // TODO: Use some threshold time to loop here to limit the
                // server from waiting forever on erroneous clients.
                Thread.yield();
                continue;
            } else if (bytesRead == -1) {
                LOGGER.finest("Client abruptly closed the connection"
                        + " while request was being processed!");
                socketChannel.close();
                // Since the channel is closed, server will not bother
                // processing this command any further even though it may have
                // received enough data to process it.
                return null;
            }
            LOGGER.finest("bytesRead: " + bytesRead);
            LOGGER.finest("data content: " + new String(dataBytes));
            totalBytesRead += bytesRead;
            if (totalBytesRead == bytesToRead) {
                break;
            }
        }
        LOGGER.finest("totalBytesRead: " + totalBytesRead);
        return dataBytes;
    }

    /**
     * Read data bytes from this socket channel until the delimiter is met.
     * The remaining data if any will be processed accordingly by their
     * respective command agents.
     *
     * @param delimiterStr Delimiter string to identify end of required data.
     * @return data bytes read from input not containing the delimiters.
     */
    private byte[] readBytesFromChannel(final String delimiterStr)
            throws IOException, InterruptedException {
        assert (delimiterStr != null);
        assert (!delimiterStr.isEmpty());

        ByteBuffer resBuffer = ByteBuffer.allocate(512);
        byte[] delimiters = delimiterStr.getBytes(AbstractCommand.CHARSET);
        int curDelimIndex = 0;
        final int endDelimIndex = delimiters.length;
        ByteBuffer buf = ByteBuffer.allocate(1);

        while (true) {
            int bytesRead = socketChannel.read(buf);
            if (bytesRead == 0 || bytesRead == -1) {
                LOGGER.finest("readBytesFromChannel bytesRead: " + bytesRead);
                break;
            }
            // Enable buf to be read.
            buf.flip();
            byte b = buf.get();
            if (b == delimiters[curDelimIndex]) {
                curDelimIndex++;
                if (curDelimIndex == endDelimIndex) {
                    // Done reading the required data
                    break;
                }
            } else if (curDelimIndex != 0) {
                // Input has partial delimiter and its disallowed by protocol.
                LOGGER.finest("Invalid client input. Has control characters");
                processErrorCommand("CLIENT_ERROR",
                        "Input cannot have control characters: "
                    + new String(resBuffer.array()));
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
     * Read the key from socketChannel and process the get request to
     * completion.
     */
    private void procesGetCommand()
            throws IOException, InterruptedException {

        byte[] keyBytes = readBytesFromChannel(/*delimiterStr=*/ "\r\n");
        String keyStr = new String(keyBytes, AbstractCommand.CHARSET);
        LOGGER.finest("processing get command: " + keyStr + " bytes: "
                + keyBytes);
        CommandGet commandGet = new CommandGet(cacheManager,
                socketChannel);
        CommandGetRequest request = new CommandGetRequest(keyBytes);
        commandGet.process(request);
    }

    private void processSetCommand() throws IOException, InterruptedException {
//        <key> <flags> <exptime> <bytes> [noreply]\r\n
//        <data block>\r\n
        LOGGER.finest("processing set command ...");

        // <key> and consume the leading space.
        byte[] keyBytes = readBytesFromChannel(/*delimiterStr=*/ " ");
        if (keyBytes == null || keyBytes.length == 0) {
            processErrorCommand("CLIENT_ERROR", "Invalid key for set command");
            return;
        }
        String keyStr = new String(keyBytes, AbstractCommand.CHARSET);
        LOGGER.finest("Thread: " + Thread.currentThread().getId()
                + " received key on server: -" + keyStr + "-" + " keyBytes: "
                + keyBytes);
        if (!validKey(keyStr)) {
            processErrorCommand("CLIENT_ERROR",
                    "Invalid key for set command: " + keyStr);
            return;
        }

        // <flags> <exptime> <bytes> [noreply]\r\n
        byte[] resBytes = readBytesFromChannel(/*delimiterStr=*/ "\r\n");
        if (resBytes == null || resBytes.length == 0) {
            processErrorCommand("CLIENT_ERROR", "Invalid set command");
            return;
        }
        String remCommandStr = new String(resBytes, AbstractCommand.CHARSET);
        LOGGER.finest("Thread: " + Thread.currentThread().getId()
                + " received on server: size: " + remCommandStr.length()
                + " Str:-" + remCommandStr + "-");

        String[] subParts = remCommandStr.split(" ");
        if (subParts.length < 3 || subParts.length > 4) {
            processErrorCommand("CLIENT_ERROR",
                    "Invalid number of arguments" + subParts.length);
            return;
        }

        short flags;
        long expTime;
        int bytes;
        try {
            flags = Short.decode(subParts[0]);
            LOGGER.finest("flags: " + flags);

            expTime = Long.parseLong(subParts[1]);
            LOGGER.finest("expTime: " + expTime);

            // Note that bytes can be 0, indicating empty data block.
            bytes = Integer.parseInt(subParts[2]);
            LOGGER.finest("bytes: " + bytes);

            if (subParts.length == 4) {
                ; // Ignore noreply flag for this assignment.
            }
        } catch (NumberFormatException e) {
            LOGGER.finest(e.getMessage());
            processErrorCommand("CLIENT_ERROR",
                    "Invalid number format on non-key arguments");
            return;
        }

        byte[] dataBytes = null;
        if (bytes != 0) {
            dataBytes = readBytesFromChannel(bytes);
            // \r\n
            readBytesFromChannel(/*bytesToRead=*/ 2);
        }

        CommandSet commandSet = new CommandSet(cacheManager,
                socketChannel);
        CommandSetRequest request = new CommandSetRequest(keyBytes, flags,
                expTime, bytes);
        commandSet.process(request, dataBytes);
    }

    private void processErrorCommand(final String status, final String msg)
            throws IOException, InterruptedException {
        LOGGER.finest("processing error command");
        if (msg != null) {
            LOGGER.finest(msg);
        }
        CommandError commandError = new CommandError(cacheManager,
                socketChannel);
        commandError.respondToClient(status, msg);
    }

    private void handleInputFromClient()
            throws IOException, InterruptedException {
        // First read 4 bytes to determine the command type.
        // Expecting: "get " or "set ".. (case sensitive)
        byte[] commandType = readBytesFromChannel(4);
        if (commandType == null || commandType.length == 0) {
            processErrorCommand("CLIENT_ERROR", "No data to process");
            return;
        }
        String commandTypeStr = new String(commandType,
                AbstractCommand.CHARSET);

        if (commandTypeStr.equals("get ")) {
            procesGetCommand();
        } else if (commandTypeStr.equals("set ")) {
            processSetCommand();
        } else {
            LOGGER.finest("Only accepts get or set command :-" + commandTypeStr
                    + "-" + " length: " + commandType.length);
            processErrorCommand("ERROR", null);
        }
        return;
    }

    @Override
    public final void run() {
        try {
            LOGGER.finest("Thread: " + Thread.currentThread().getId()
                    + " running ...");
            handleInputFromClient();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (InvalidParameterException e) {
            try {
                processErrorCommand("CLIENT_ERROR", e.getMessage());
            } catch (IOException e1) {
                LOGGER.finer(e.getMessage());
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        } catch (Exception e) {
            // Any other exceptions from handling the input goes out as server
            // error.
            LOGGER.finer(e.getMessage());
            try {
                processErrorCommand("SERVER_ERROR", e.getMessage());
            } catch (IOException e1) {
                LOGGER.finer("IOException processing error command: "
                        + e1.getMessage());
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException e2) {
                    LOGGER.finer("IOException trying to close socketChannel"
                            + e2.getMessage());
                }
                selectionKey.cancel();
            }
        } finally {
            // Restore READS on this key. Client should be able to write data
            // into this channel.
            if (selectionKey.isValid()) {
                selectionKey.interestOps(SelectionKey.OP_READ);
            }
        }
    }
}
