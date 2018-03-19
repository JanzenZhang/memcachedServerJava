/**
 * @author Dilip Simha
 */
package server.cache;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** cache value as stored in cache. */
public final class CacheValue {
    private final short flags;

    private final int bytes;

    /** data can be empty (null). */
    private final byte[] data;

    public CacheValue(final short flags, final int bytes, final byte[] data) {
        assert ((bytes == 0) || (data != null));
        assert ((bytes == 0) || (bytes == data.length));

        this.flags = flags;
        this.bytes = bytes;
        this.data = data;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        CacheValue compObj = (CacheValue) obj;
        return ((compObj.flags == this.flags) && (compObj.bytes == this.bytes)
                && Arrays.equals(compObj.data, this.data));
    }

    @Override
    public int hashCode() {
        return Objects.hash(flags, bytes, data);
    }

    public int getSerializedSize() {
        return Short.BYTES + Integer.BYTES + bytes;
    }

    public short getFlag() {
        return flags;
    }

    public int getBytes() {
        return bytes;
    }

    public byte[] getData() {
        return data;
    }

    /**
     * Deserialize the byte array memory dump and convert it into CacheValue
     * object.
     *
     * @param memDump byte array wrapped into ByteBuffer.
     * @return CacheValue object if deserialized; null otherwise.
     */
    public static CacheValue deserialize(final ByteBuffer buf)
            throws BufferUnderflowException {
        short flags = buf.getShort();
        int bytes = buf.getInt();
        byte[] data = null;
        if (bytes != 0) {
            data = new byte[bytes];
            buf.get(data);
        }
        return new CacheValue(flags, bytes, data);
    }

    /**
     * Serialize the cacheValue object into byte array.
     *
     * @param cacheValue Object to serialize.
     * @param buf byte array memory dump of the cacheValue object wrapped into
     *              ByteBuffer.
     */
    public static void serialize(final CacheValue cacheValue,
            final ByteBuffer buf) throws BufferOverflowException  {
        buf.putShort(cacheValue.flags)
            .putInt(cacheValue.bytes);
        if (cacheValue.bytes != 0) {
            buf.put(cacheValue.data);
        }
        return;
    }
}
