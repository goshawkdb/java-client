package io.goshawkdb.client;

import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.goshawkdb.client.ConnectionFactory.KEY_LEN;

/**
 * Representation of GoshawkDB Object Ids. Every object within GoshawkDB has a unique id.
 */
public class VarUUId {

    final byte[] id;

    VarUUId(final ByteBuffer buf) {
        id = new byte[KEY_LEN];
        buf.get(id, 0, KEY_LEN);
    }

    @Override
    public String toString() {
        return "VarUUId:" + Hex.encodeHexString(id);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj != null && obj instanceof VarUUId && Arrays.equals(id, ((VarUUId) obj).id));
    }

    /**
     * Returns the object identifier as a byte array. This can be useful for example when using the collections library and you
     * wish to use GoshawkDB Objects as keys in a collection.
     *
     * @return The object identifier as a byte array.
     */
    public byte[] asBytes() {
        return id.clone();
    }
}
