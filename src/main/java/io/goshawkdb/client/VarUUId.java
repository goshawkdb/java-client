package io.goshawkdb.client;

import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.goshawkdb.client.ConnectionFactory.KEY_LEN;

public class VarUUId {

    public final byte[] id;

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
        if (obj != null && obj instanceof VarUUId) {
            return Arrays.equals(id, ((VarUUId) obj).id);
        }
        return false;
    }
}
