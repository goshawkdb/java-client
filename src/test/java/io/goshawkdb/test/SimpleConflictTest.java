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

public class SimpleConflictTest extends TestBase {
    public SimpleConflictTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void simpleConflict() throws InterruptedException {
        try {
            final long limit = 1000;
            final int parCount = 5;
            final int objCount = 3;

            final ByteBuffer rootGuid = setRootToNZeroObjs(createConnections(1)[0], objCount);

            inParallel(parCount, (final int tId, final Connection c) -> {
                final RefCap[] rootRefs = awaitRootVersionChange(c, rootGuid, objCount);
                long expected = 0L;
                final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                while (expected <= limit) {
                    final long expectedCopy = expected;
                    final long read = c.transact(txn -> {
                        System.out.println("" + tId + ": starting with expected " + expectedCopy);
                        final ValueRefs vr0 = txn.read(rootRefs[0]);
                        if (txn.restartNeeded()) {
                            return null;
                        }
                        final long val = vr0.value.order(ByteOrder.BIG_ENDIAN).getLong(0);
                        if (val > limit) {
                            return val;
                        }
                        buf.putLong(0, val + 1);
                        txn.write(rootRefs[0], buf);
                        if (txn.restartNeeded()) {
                            return null;
                        }
                        for (int idx = 1; idx < rootRefs.length; idx++) {
                            final ValueRefs vrI = txn.read(rootRefs[idx]);
                            if (txn.restartNeeded()) {
                                return null;
                            }
                            final long valI = vrI.value.order(ByteOrder.BIG_ENDIAN).getLong(0);
                            if (val == valI) {
                                txn.write(rootRefs[idx], buf);
                                if (txn.restartNeeded()) {
                                    return null;
                                }
                            } else {
                                fail("" + tId + ": Object 0 has value " + val + " but " + idx + " has value " + valI);
                            }
                        }
                        return val + 1;
                    }).getResultOrRethrow();
                    if (read < expected) {
                        fail("" + tId + ": expected to read " + expected + " but read " + read);
                    }
                    expected = read + 1;
                }
            });
        } finally {
            shutdown();
        }
    }


}
