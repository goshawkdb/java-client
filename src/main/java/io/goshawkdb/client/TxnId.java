package io.goshawkdb.client;

import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.goshawkdb.client.ConnectionFactory.KEY_LEN;


public class TxnId {
    public final byte[] id;

    TxnId(final byte[] id) {
        this.id = id;
    }

    TxnId(final ByteBuffer buf) {
        id = new byte[KEY_LEN];
        buf.get(id, 0, KEY_LEN);
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
