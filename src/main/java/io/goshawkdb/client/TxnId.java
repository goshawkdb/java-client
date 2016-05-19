package io.goshawkdb.client;

import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;


public class TxnId {
    public final byte[] id;

    public TxnId(final byte[] id) {
        this.id = id;
    }

    @Override
    public String toString() {
        final String all = Hex.encodeHexString(id);
        return String.format("TxnId:%s-%s-%s-%s", all.substring(0, 16), all.substring(16, 24), all.substring(24, 32), all.substring(32, 40));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof TxnId) {
            return Arrays.equals(id, ((TxnId) obj).id);
        }
        return false;
    }
}
