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
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TransactionResult;
import io.goshawkdb.client.TxnId;
import io.goshawkdb.client.VarUUId;

public class ParCountTest extends TestBase {

    public ParCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void parCount() throws Exception {
        try {
            final int threadCount = 8;
            final TxnId origRootVsn = setRootToNZeroObjs(createConnections(1)[0], threadCount);

            inParallel(threadCount, (final int tId, final Connection c, final Queue<Exception> exceptionQ) -> {
                awaitRootVersionChange(c, origRootVsn);
                final VarUUId objId = c.runTransaction((final Transaction<VarUUId> txn) ->
                        txn.getRoot().getReferences()[tId].id
                ).result;
                final long start = System.nanoTime();
                long expected = 0L;
                for (int idx = 0; idx < 1000; idx++) {
                    final long expectedCopy = expected;
                    expected = c.runTransaction((final Transaction<Long> txn) -> {
                        final GoshawkObj obj = txn.getObject(objId);
                        final ByteBuffer valBuf = obj.getValue().order(ByteOrder.BIG_ENDIAN);
                        final long old = valBuf.getLong(0);
                        if (old == expectedCopy) {
                            final long val = old + 1;
                            obj.set(valBuf.putLong(0, val));
                            return val;
                        } else {
                            throw new IllegalStateException("" + tId + ": Expected " + expectedCopy + " but found " + old);
                        }
                    }).result;
                }
                final long end = System.nanoTime();
                System.out.println("" + tId + ": Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
            });
        } finally {
            shutdown();
        }
    }
}
