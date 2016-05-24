package io.goshawkdb.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;

public class SoloCountTest extends TestBase {

    public SoloCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void test() throws Throwable {
        final Connection conn = createConnections(1)[0];
        final long start = System.nanoTime();
        long expected = 0L;
        for (int idx = 0; idx < 1000; idx++) {
            final int idy = idx;
            final long expectedCopy = expected;
            expected = conn.runTransaction((txn) -> {
                final GoshawkObj root = txn.getRoot();
                if (idy == 0) {
                    root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0));
                    return 0L;
                } else {
                    final long old = root.getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                    if (old == expectedCopy) {
                        final long val = old + 1;
                        root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0, val));
                        return val;
                    } else {
                        throw new IllegalStateException("Expected " + expectedCopy + " but found " + old);
                    }
                }
            });
        }
        final long end = System.nanoTime();
        System.out.println("Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
        conn.close();
        shutdownGracefully();
    }
}
