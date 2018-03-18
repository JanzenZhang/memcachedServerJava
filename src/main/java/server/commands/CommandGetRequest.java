/**
 * @author Dilip Simha
 */
package server.commands;

/** Arguments for "get" command. */
public final class CommandGetRequest {
    private final byte[] key;

    public CommandGetRequest(byte[] key) {
        // Key length should be lesser than 350 bytes as per memcached
        // protocol.
        assert(key != null);
        assert (key.length <= 250);

        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }
}
