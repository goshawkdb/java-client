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
        final Connection conn = factory.connect(certs, "localhost", 10001);
        final long start = System.nanoTime();
        for (int idx = 0; idx < 1000; idx++) {
            final int idy = idx;
            long result = conn.runTransaction((txn) -> {
                final GoshawkObj root = txn.getRoot();
                if (idy == 0) {
                    root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0));
                    return 0L;
                } else {
                    final long val = 1 + root.getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                    root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0, val));
                    return val;
                }
            });
        }
        final long end = System.nanoTime();
        System.out.println("Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
        conn.close();
        factory.group.shutdownGracefully();
    }
}
