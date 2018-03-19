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
    final boolean noReply;

    public CommandSetRequest(final byte[] key, final short flags,
            final long expTime, final int bytes) {
        this(key, flags, expTime, bytes, /*noReply*/ false);
    }

    public CommandSetRequest(final byte[] key, final short flags,
            final long expTime, final int bytes, final boolean noReply) {
        this.key = key;
        this.flags = flags;
        this.expTime = expTime;
        this.bytes = bytes;
        this.noReply = noReply;
    }
}
