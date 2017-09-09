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

public class PingPongTest extends TestBase {

    public PingPongTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void pingPong() throws Exception {
        try {
            final int limit = 1000;
            final int threadCount = 4;
            final ByteBuffer rootGuid = setRootToNZeroObjs(createConnections(1)[0], 1);

            inParallel(threadCount, (final int tId, final Connection c) -> {
                final RefCap[] rootRefs = awaitRootVersionChange(c, rootGuid, 1);
                RefCap objRef = rootRefs[0];
                boolean inProgress = true;
                while (inProgress) {
                    inProgress = c.transact(txn -> {
                        final ValueRefs vr = txn.read(objRef);
                        if (txn.restartNeeded()) {
                            return null;
                        }
                        final ByteBuffer valBuf = vr.value.order(ByteOrder.BIG_ENDIAN);
                        final long val = valBuf.getLong(0);
                        if (val > limit) {
                            return false;
                        } else if (val % threadCount == tId) {
                            System.out.println("" + tId + " incrementing at " + val);
                            txn.write(objRef, valBuf.putLong(0, val + 1));
                        } else {
                            txn.retry();
                            return null;
                        }
                        return true;
                    }).getResultOrRethrow();
                }
            });
        } finally {
            shutdown();
        }
    }
}
