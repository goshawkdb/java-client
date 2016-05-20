package io.goshawkdb.client;

import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public class VarUUId {
    public final byte[] id;

    VarUUId(final byte[] id) {
        this.id = id;
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
