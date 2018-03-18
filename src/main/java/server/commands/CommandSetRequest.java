/**
 * @author Dilip Simha
 */
package server.commands;

/**
 * Arguments for "set" command. Note that the actual data will be extracted
 * later.
 */
public final class CommandSetRequest {
    final byte[] key;
    final short flags;
    final long expTime;
    final int bytes;
    boolean noReply;

    public CommandSetRequest(byte[] key, short flags, long expTime, int bytes) {
        this(key, flags, expTime, bytes, /*noReply*/ false);
    }

    public CommandSetRequest(byte[] key, short flags, long expTime, int bytes,
            boolean noReply) {
        this.key = key;
        this.flags = flags;
        this.expTime = expTime;
        this.bytes = bytes;
        this.noReply = noReply;
    }
}
