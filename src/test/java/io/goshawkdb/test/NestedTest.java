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
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TxnId;

public class NestedTest extends TestBase {

    public NestedTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void nestedRead() throws Exception {
        try {
            final Connection c = createConnections(1)[0];

            // Just read the root var from several nested txns
            final int r0 = c.runTransaction((final Transaction<Integer> t0) -> {
                final GoshawkObj rootObj0 = t0.getRoot();
                final int r1 = c.runTransaction((final Transaction<Integer> t1) -> {
                    final GoshawkObj rootObj1 = t1.getRoot();
                    if (rootObj0 != rootObj1) {
                        throw new IllegalStateException("Should have pointer equality between the same object in nested txns");
                    }
                    final int r2 = c.runTransaction((final Transaction<Integer> t2) -> {
                        final GoshawkObj rootObj2 = t2.getRoot();
                        if (rootObj1 != rootObj2) {
                            throw new IllegalStateException("Should have pointer equality between the same object in nested txns");
                        }
                        return 42;
                    }).result;
                    if (r2 != 42) {
                        throw new IllegalStateException("Expecting to get 42 back from nested txn but got " + r2);
                    }
                    return 43;
                }).result;
                if (r1 != 43) {
                    throw new IllegalStateException("Expecting to get 43 back from nested txn but got " + r1);
                }
                return 44;
            }).result;
            if (r0 != 44) {
                throw new IllegalStateException("Expecting to get 44 back from nested txn but got " + r0);
            }
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedWrite() throws Exception {
        try {
            final Connection c = createConnections(1)[0];

            // A write made in a parent should be visible in the child
            c.runTransaction((final Transaction<Void> t0) -> {
                final GoshawkObj rootObj0 = t0.getRoot();
                rootObj0.set(ByteBuffer.wrap("outer".getBytes()));
                c.runTransaction((final Transaction<Void> t1) -> {
                    final GoshawkObj rootObj1 = t1.getRoot();
                    final String found1 = byteBufferToString(rootObj1.getValue(), "outer".length());
                    if (!"outer".equals(found1)) {
                        throw new IllegalStateException("Expected to find 'outer' but found " + found1);
                    }
                    rootObj1.set(ByteBuffer.wrap("mid".getBytes()));
                    c.runTransaction((final Transaction<Void> t2) -> {
                        final GoshawkObj rootObj2 = t2.getRoot();
                        final String found2 = byteBufferToString(rootObj2.getValue(), "mid".length());
                        if (!"mid".equals(found2)) {
                            throw new IllegalStateException("Expected to find 'mid' but found " + found2);
                        }
                        rootObj1.set(ByteBuffer.wrap("inner".getBytes()));
                        return null;
                    });
                    final String found3 = byteBufferToString(rootObj1.getValue(), "inner".length());
                    if (!"inner".equals(found3)) {
                        throw new IllegalStateException("Expected to find 'inner' but found " + found3);
                    }
                    return null;
                });
                final String found4 = byteBufferToString(rootObj0.getValue(), "inner".length());
                if (!"inner".equals(found4)) {
                    throw new IllegalStateException("Expected to find 'inner' but found " + found4);
                }
                return null;
            });
        } finally {
            shutdown();
        }
    }

    private static class UserAbortedTxnException extends Exception {
    }

    @Test
    public void nestedInnerAbort() throws Exception {
        try {
            final Connection c = createConnections(1)[0];

            // A write made in a child which is aborted should not be seen in the parent.
            c.runTransaction((final Transaction<Void> t0) -> {
                final GoshawkObj rootObj0 = t0.getRoot();
                rootObj0.set(ByteBuffer.wrap("outer".getBytes()));
                c.runTransaction((final Transaction<Void> t1) -> {
                    final GoshawkObj rootObj1 = t1.getRoot();
                    final String found1 = byteBufferToString(rootObj1.getValue(), "outer".length());
                    if (!"outer".equals(found1)) {
                        throw new IllegalStateException("Expected to find 'outer' but found " + found1);
                    }
                    rootObj1.set(ByteBuffer.wrap("mid".getBytes()));
                    try {
                        c.runTransaction((final Transaction<Void> t2) -> {
                            final GoshawkObj rootObj2 = t2.getRoot();
                            final String found2 = byteBufferToString(rootObj2.getValue(), "mid".length());
                            if (!"mid".equals(found2)) {
                                throw new IllegalStateException("Expected to find 'mid' but found " + found2);
                            }
                            rootObj1.set(ByteBuffer.wrap("inner".getBytes()));
                            throw new UserAbortedTxnException();
                        });
                    } catch (final UserAbortedTxnException e) {
                    }
                    final String found3 = byteBufferToString(rootObj1.getValue(), "mid".length());
                    if (!"mid".equals(found3)) {
                        throw new IllegalStateException("Expected to find 'mid' but found " + found3);
                    }
                    return null;
                });
                final String found4 = byteBufferToString(rootObj0.getValue(), "mid".length());
                if (!"mid".equals(found4)) {
                    throw new IllegalStateException("Expected to find 'mid' but found " + found4);
                }
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
                    c.runTransaction((final Transaction<Void> txn) -> {
                        txn.getRoot().set(ByteBuffer.wrap("magic".getBytes()));
                        return null;
                    });

                } else {
                    c.runTransaction((final Transaction<Void> t0) -> {
                        final GoshawkObj rootObj0 = t0.getRoot();
                        final String found0 = byteBufferToString(rootObj0.getValue(), "magic".length());
                        if ("magic".equals(found0)) {
                            return null;
                        } else {
                            return c.runTransaction((final Transaction<Void> t1) -> {
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
    public void nestedInnerCreate() throws Exception {
        try {
            // A create made in a child, returned to the parent should both be
            // directly usable and writable.
            final Connection c = createConnections(1)[0];
            c.runTransaction((final Transaction<Void> t0) -> {
                final GoshawkObj rootObj0 = t0.getRoot();
                final GoshawkObj obj0 = c.runTransaction((final Transaction<GoshawkObj> t1) -> {
                    final GoshawkObj obj1 = t1.createObject(ByteBuffer.wrap("Hello".getBytes()));
                    t1.getRoot().setReferences(obj1);
                    return obj1;
                }).result;
                final GoshawkObj[] refs = rootObj0.getReferences();
                if (refs.length != 1 || refs[0] != obj0) {
                    throw new IllegalStateException("Expected to find obj0 as only ref from root. Instead found " + refs);
                }
                final String val0 = byteBufferToString(refs[0].getValue(), "Hello".length());
                if (!"Hello".equals(val0)) {
                    throw new IllegalStateException("Expected to find 'Hello' as value of obj0. Instead found " + val0);
                }
                obj0.set(ByteBuffer.wrap("Goodbye".getBytes()));
                return null;
            });

            final String val1 = c.runTransaction((final Transaction<String> t0) ->
                    byteBufferToString(t0.getRoot().getReferences()[0].getValue(), "Goodbye".length())
            ).result;
            if (!"Goodbye".equals(val1)) {
                throw new IllegalStateException("Expected to find 'Gooodbye' as value of obj0. Instead found " + val1);
            }
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
