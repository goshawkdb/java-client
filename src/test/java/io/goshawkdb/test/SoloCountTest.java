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
import io.goshawkdb.client.GoshawkObjRef;

import static org.junit.Assert.fail;

public class SoloCountTest extends TestBase {

    public SoloCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void soloCount() throws InterruptedException {
        try {
            final Connection c = createConnections(1)[0];
            setRootToZeroInt64(c);
            final long start = System.nanoTime();
            long expected = 0L;
            for (int idx = 0; idx < 1000; idx++) {
                final long expectedCopy = expected;
                expected = runTransaction(c, txn -> {
                    final GoshawkObjRef root = getRoot(txn);
                    final ByteBuffer valBuf = root.getValue().order(ByteOrder.BIG_ENDIAN);
                    final long old = valBuf.getLong(0);
                    if (old == expectedCopy) {
                        final long val = old + 1;
                        root.set(valBuf.putLong(0, val));
                        return val;
                    } else {
                        fail("Expected " + expectedCopy + " but found " + old);
                        return null;
                    }
                });
            }
            final long end = System.nanoTime();
            System.out.println("Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
        } finally {
            shutdown();
        }
    }
}
