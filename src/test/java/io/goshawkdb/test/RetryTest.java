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
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.TxnId;

import static org.junit.Assert.fail;

public class RetryTest extends TestBase {
    public RetryTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    // This tests that multiple retrying txns are all woken up by a single write.
    @Test
    public void simpleRetry() throws Exception {
        try {
            final long magicNumber = 42L;
            final int retriers = 8;
            final TxnId origRootVsn = setRootToZeroInt64(createConnections(1)[0]);
            final CountDownLatch retryLatch = new CountDownLatch(retriers);
            final CountDownLatch successLatch = new CountDownLatch(retriers);

            inParallel(retriers + 1, (final int tId, final Connection c, final Queue<Exception> exceptionQ) -> {
                awaitRootVersionChange(c, origRootVsn);

                if (tId == 0) {
                    retryLatch.await();
                    Thread.sleep(250);
                    System.out.println("All retriers have retried. Going to modify value.");
                    runTransaction(c, txn -> {
                        getRoot(txn).set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0, magicNumber));
                        return null;
                    });

                } else {
                    final AtomicBoolean triggered = new AtomicBoolean(false);
                    final long found = runTransaction(c, txn -> {
                        final long num = getRoot(txn).getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                        if (num == 0) {
                            if (!triggered.get()) {
                                triggered.set(true);
                                System.out.println("" + tId + ": going to retry (wasn't triggered).");
                                retryLatch.countDown();
                            } else {
                                System.out.println("" + tId + ": going to retry (was triggered).");
                            }
                            txn.retry();
                        } else if (!triggered.get()) {
                            fail("" + tId + ": found " + num + " before I triggered!");
                        }
                        System.out.println("" + tId + ": found non-zero: " + num);
                        return num;
                    });
                    if (found != magicNumber) {
                        fail("" + tId + ": expected to find " + magicNumber + " but found " + found);
                    }
                    successLatch.countDown();
                }
            });

            successLatch.await();
            System.out.println("All retriers have found the right value.");
        } finally {
            shutdown();
        }
    }

    // This tests that a retry on several different objects is awoken by a write to one of them.
    @Test
    public void disjointRetry() throws Exception {
        try {
            final long magicNumber = 42;
            final int changeIdx = 2;
            final TxnId origRootVsn = setRootToNZeroObjs(createConnections(1)[0], 3);

            final CountDownLatch retryLatch = new CountDownLatch(1);
            inParallel(2, (final int tId, final Connection c, final Queue<Exception> exceptionQ) -> {
                awaitRootVersionChange(c, origRootVsn);

                if (tId == 0) {
                    retryLatch.await();
                    Thread.sleep(250);
                    runTransaction(c, txn -> {
                        final GoshawkObjRef obj = getRoot(txn).getReferences()[changeIdx];
                        obj.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0, magicNumber));
                        return null;
                    });

                } else {
                    final AtomicBoolean triggered = new AtomicBoolean(false);
                    final int foundIdx = runTransaction(c, txn -> {
                        final GoshawkObjRef[] objs = getRoot(txn).getReferences();
                        for (int idx = 0; idx < objs.length; idx++) {
                            final GoshawkObjRef obj = objs[idx];
                            final long v = obj.getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                            if (v != 0) {
                                return idx;
                            }
                        }
                        if (!triggered.get()) {
                            triggered.set(true);
                            retryLatch.countDown();
                        }
                        txn.retry();
                        fail("" + tId + ": Reached unreachable code");
                        return null;
                    });
                    if (foundIdx != changeIdx) {
                        fail("" + tId + ": Expected to find " + changeIdx + " had changed, but actually found " + foundIdx);
                    }
                }
            });
        } finally {
            shutdown();
        }

    }
}
