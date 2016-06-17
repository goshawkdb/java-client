package io.goshawkdb.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.TransactionAbortedException;
import io.goshawkdb.client.TxnId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class NestedTest extends TestBase {

    public NestedTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void nestedRead() throws InterruptedException {
        try {
            final Connection c = createConnections(1)[0];

            // Just read the root var from several nested txns
            final int r0 = c.runTransaction(t0 -> {
                final GoshawkObj rootObj0 = getRoot(t0);
                assertNotNull(rootObj0);
                final int r1 = c.runTransaction(t1 -> {
                    final GoshawkObj rootObj1 = getRoot(t1);
                    assertSame("Should have pointer equality between the same object in nested txns", rootObj0, rootObj1);
                    final int r2 = c.runTransaction(t2 -> {
                        final GoshawkObj rootObj2 = getRoot(t2);
                        assertSame("Should have pointer equality between the same object in nested txns", rootObj1, rootObj2);
                        return 42;
                    }).result;
                    assertEquals("Expecting to get 42 back from nested txn but got " + r2, 42, r2);
                    return 43;
                }).result;
                assertEquals("Expecting to get 43 back from nested txn but got " + r1, 43, r1);
                return 44;
            }).result;
            assertEquals("Expecting to get 44 back from nested txn but got " + r0, 44, r0);
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedWrite() throws InterruptedException {
        try {
            final Connection c = createConnections(1)[0];

            // A write made in a parent should be visible in the child
            c.runTransaction(t0 -> {
                final GoshawkObj rootObj0 = getRoot(t0);
                assertNotNull(rootObj0);
                rootObj0.set(ByteBuffer.wrap("outer".getBytes()));
                c.runTransaction(t1 -> {
                    final GoshawkObj rootObj1 = getRoot(t1);
                    final String found1 = byteBufferToString(rootObj1.getValue(), "outer".length());
                    assertEquals("Expected to find 'outer' but found " + found1, "outer", found1);
                    rootObj1.set(ByteBuffer.wrap("mid".getBytes()));
                    c.runTransaction(t2 -> {
                        final GoshawkObj rootObj2 = getRoot(t2);
                        final String found2 = byteBufferToString(rootObj2.getValue(), "mid".length());
                        assertEquals("Expected to find 'mid' but found " + found2, "mid", found2);
                        rootObj1.set(ByteBuffer.wrap("inner".getBytes()));
                        return null;
                    });
                    final String found3 = byteBufferToString(rootObj1.getValue(), "inner".length());
                    assertEquals("Expected to find 'inner' but found " + found3, "inner", found3);
                    return null;
                });
                final String found4 = byteBufferToString(rootObj0.getValue(), "inner".length());
                assertEquals("Expected to find 'inner' but found " + found4, "inner", found4);
                return null;
            });
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedInnerAbort() throws InterruptedException {
        try {
            final Connection c = createConnections(1)[0];

            // A write made in a child which is aborted should not be seen in the parent.
            c.runTransaction(t0 -> {
                final GoshawkObj rootObj0 = getRoot(t0);
                assertNotNull(rootObj0);
                rootObj0.set(ByteBuffer.wrap("outer".getBytes()));
                c.runTransaction(t1 -> {
                    final GoshawkObj rootObj1 = getRoot(t1);
                    final String found1 = byteBufferToString(rootObj1.getValue(), "outer".length());
                    assertEquals("Expected to find 'outer' but found " + found1, "outer", found1);
                    rootObj1.set(ByteBuffer.wrap("mid".getBytes()));
                    final boolean aborted = c.runTransaction(t2 -> {
                        final GoshawkObj rootObj2 = getRoot(t2);
                        final String found2 = byteBufferToString(rootObj2.getValue(), "mid".length());
                        assertEquals("Expected to find 'mid' but found " + found2, "mid", found2);
                        rootObj1.set(ByteBuffer.wrap("inner".getBytes()));
                        throw TransactionAbortedException.e;
                    }).isAborted();
                    final String found3 = byteBufferToString(rootObj1.getValue(), "mid".length());
                    assertEquals("Expected to find 'mid' but found " + found3, "mid", found3);
                    return null;
                });
                final String found4 = byteBufferToString(rootObj0.getValue(), "mid".length());
                assertEquals("Expected to find 'mid' but found " + found4, "mid", found4);
                return null;
            });
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedInnerRetry() throws Exception {
        try {
            final TxnId origRootVsn = setRootToZeroInt64(createConnections(1)[0]);
            final CountDownLatch retryLatch = new CountDownLatch(1);
            inParallel(2, (final int tId, final Connection c, final Queue<Exception> exceptionQ) -> {
                if (tId == 0) {
                    awaitRootVersionChange(c, origRootVsn);
                    retryLatch.await();
                    Thread.sleep(250);
                    c.runTransaction(txn -> {
                        getRoot(txn).set(ByteBuffer.wrap("magic".getBytes()));
                        return null;
                    });

                } else {
                    c.runTransaction(t0 -> {
                        final GoshawkObj rootObj0 = getRoot(t0);
                        assertNotNull(rootObj0);
                        final String found0 = byteBufferToString(rootObj0.getValue(), "magic".length());
                        if ("magic".equals(found0)) {
                            return null;
                        } else {
                            return c.runTransaction(t1 -> {
                                // Even though we've not read root in this inner txn,
                                // retry should still work!
                                retryLatch.countDown();
                                t1.retry();
                                return null;
                            }).result;
                        }
                    });
                }
            });
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedInnerCreate() throws InterruptedException {
        try {
            // A create made in a child, returned to the parent should both be
            // directly usable and writable.
            final Connection c = createConnections(1)[0];
            c.runTransaction(t0 -> {
                final GoshawkObj rootObj0 = getRoot(t0);
                final GoshawkObj obj0 = c.runTransaction(t1 -> {
                    final GoshawkObj obj1 = t1.createObject(ByteBuffer.wrap("Hello".getBytes()));
                    getRoot(t1).set(null, obj1);
                    return obj1;
                }).result;
                final GoshawkObj[] refs = rootObj0.getReferences();
                if (refs.length != 1 || refs[0] != obj0) {
                    fail("Expected to find obj0 as only ref from root. Instead found " + refs);
                }
                final String val0 = byteBufferToString(refs[0].getValue(), "Hello".length());
                assertEquals("Expected to find 'Hello' as value of obj0. Instead found " + val0, "Hello", val0);
                obj0.set(ByteBuffer.wrap("Goodbye".getBytes()));
                return null;
            });

            final String val1 = c.runTransaction(t0 ->
                    byteBufferToString(getRoot(t0).getReferences()[0].getValue(), "Goodbye".length())
            ).result;
            assertEquals("Expected to find 'Goodbye' as value of obj0. Instead found " + val1, "Goodbye", val1);
        } finally {
            shutdown();
        }
    }

    private static String byteBufferToString(final ByteBuffer buf, final int len) {
        final byte[] ary = new byte[len];
        buf.get(ary);
        return new String(ary);
    }
}
