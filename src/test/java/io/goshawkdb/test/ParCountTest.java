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
import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.ValueRefs;

import static org.junit.Assert.fail;

public class ParCountTest extends TestBase {

    public ParCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void parCount() throws Exception {
        try {
            final int threadCount = 8;
            final ByteBuffer rootGuid = setRootToNZeroObjs(createConnections(1)[0], threadCount);

            inParallel(threadCount, (final int tId, final Connection c) -> {
                final RefCap[] rootRefs = awaitRootVersionChange(c, rootGuid, threadCount);
                final RefCap objRef = rootRefs[tId];
                final long start = System.nanoTime();
                long expected = 0L;
                for (int idx = 0; idx < 1000; idx++) {
                    final long expectedCopy = expected;
                    expected = c.transact(txn -> {
                        final ValueRefs vr = txn.read(objRef);
                        if (txn.restartNeeded()) {
                            return null;
                        }
                        final ByteBuffer valBuf = vr.value.order(ByteOrder.BIG_ENDIAN);
                        final long old = valBuf.getLong(0);
                        if (old == expectedCopy) {
                            final long val = old + 1;
                            txn.write(objRef, valBuf.putLong(0, val));
                            return val;
                        } else {
                            fail("" + tId + ": Expected " + expectedCopy + " but found " + old);
                            return null;
                        }
                    }).getResultOrRethrow();
                }
                final long end = System.nanoTime();
                System.out.println("" + tId + ": Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
            });
        } finally {
            shutdown();
        }
    }
}
