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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.ValueRefs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NestedTest extends TestBase {

    public NestedTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void nestedRead() throws InterruptedException {
        try {
            final Connection c = createConnections(1)[0];

            // Just read the root var from several nested txns
            final int r0 = c.transact(t0 -> {
                final RefCap rootObj0 = t0.root(rootName);
                assertNotNull(rootObj0);
                final int r1 = t0.transact(t1 -> {
                    final RefCap rootObj1 = t1.root(rootName);
                    assertTrue("Should have pointer equality between the same object in nested txns", rootObj0.sameReferent(rootObj1));
                    final int r2 = t1.transact(t2 -> {
                        final RefCap rootObj2 = t2.root(rootName);
                        assertTrue("Should have pointer equality between the same object in nested txns", rootObj1.sameReferent(rootObj2));
                        return 42;
                    }).getResultOrRethrow();
                    assertEquals("Expecting to get 42 back from nested txn but got " + r2, 42, r2);
                    return 43;
                }).getResultOrRethrow();
                assertEquals("Expecting to get 43 back from nested txn but got " + r1, 43, r1);
                return 44;
            }).getResultOrRethrow();
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
            assertTrue(
                    c.transact(t0 -> {
                        final RefCap rootObj0 = t0.root(rootName);
                        assertNotNull(rootObj0);
                        t0.write(rootObj0, ByteBuffer.wrap("outer".getBytes()));
                        if (t0.restartNeeded()) {
                            return null;
                        }
                        assertTrue(
                                t0.transact(t1 -> {
                                    final RefCap rootObj1 = t1.root(rootName);
                                    ValueRefs vr1 = t1.read(rootObj1);
                                    if (t1.restartNeeded()) {
                                        return null;
                                    }
                                    final String found1 = byteBufferToString(vr1.value, "outer".length());
                                    assertEquals("Expected to find 'outer' but found " + found1, "outer", found1);
                                    t1.write(rootObj1, ByteBuffer.wrap("mid".getBytes()));
                                    if (t1.restartNeeded()) {
                                        return null;
                                    }
                                    assertTrue(
                                            t1.transact(t2 -> {
                                                final RefCap rootObj2 = t2.root(rootName);
                                                final ValueRefs vr2 = t2.read(rootObj1);
                                                if (t2.restartNeeded()) {
                                                    return null;
                                                }
                                                final String found2 = byteBufferToString(vr2.value, "mid".length());
                                                assertEquals("Expected to find 'mid' but found " + found2, "mid", found2);
                                                t2.write(rootObj2, ByteBuffer.wrap("inner".getBytes()));
                                                return null;
                                            }).isSuccessful());
                                    vr1 = t1.read(rootObj1);
                                    if (t1.restartNeeded()) {
                                        return null;
                                    }
                                    final String found3 = byteBufferToString(vr1.value, "inner".length());
                                    assertEquals("Expected to find 'inner' but found " + found3, "inner", found3);
                                    return null;
                                }).isSuccessful());
                        final ValueRefs vr0 = t0.read(rootObj0);
                        if (t0.restartNeeded()) {
                            return null;
                        }
                        final String found4 = byteBufferToString(vr0.value, "inner".length());
                        assertEquals("Expected to find 'inner' but found " + found4, "inner", found4);
                        return null;
                    }).isSuccessful());
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedInnerAbort() throws InterruptedException {
        try {
            final Connection c = createConnections(1)[0];

            // A write made in a child which is aborted should not be seen in the parent.
            assertTrue(
                    c.transact(t0 -> {
                        final RefCap rootObj0 = t0.root(rootName);
                        assertNotNull(rootObj0);
                        t0.write(rootObj0, ByteBuffer.wrap("outer".getBytes()));
                        if (t0.restartNeeded()) {
                            return null;
                        }
                        assertTrue(
                                t0.transact(t1 -> {
                                    final RefCap rootObj1 = t1.root(rootName);
                                    ValueRefs vr1 = t1.read(rootObj1);
                                    if (t1.restartNeeded()) {
                                        return null;
                                    }
                                    final String found1 = byteBufferToString(vr1.value, "outer".length());
                                    assertEquals("Expected to find 'outer' but found " + found1, "outer", found1);
                                    t1.write(rootObj1, ByteBuffer.wrap("mid".getBytes()));
                                    if (t1.restartNeeded()) {
                                        return null;
                                    }
                                    assertFalse("Expected successful to be false",
                                            t1.transact(t2 -> {
                                                final RefCap rootObj2 = t2.root(rootName);
                                                final ValueRefs vr2 = t2.read(rootObj2);
                                                if (t2.restartNeeded()) {
                                                    return null;
                                                }
                                                final String found2 = byteBufferToString(vr2.value, "mid".length());
                                                assertEquals("Expected to find 'mid' but found " + found2, "mid", found2);
                                                t2.write(rootObj2, ByteBuffer.wrap("inner".getBytes()));
                                                t2.abort();
                                                return null;
                                            }).isSuccessful());
                                    vr1 = t1.read(rootObj1);
                                    if (t1.restartNeeded()) {
                                        return null;
                                    }
                                    final String found3 = byteBufferToString(vr1.value, "mid".length());
                                    assertEquals("Expected to find 'mid' but found " + found3, "mid", found3);
                                    return null;
                                }).isSuccessful());
                        final ValueRefs vr0 = t0.read(rootObj0);
                        if (t0.restartNeeded()) {
                            return null;
                        }
                        final String found4 = byteBufferToString(vr0.value, "mid".length());
                        assertEquals("Expected to find 'mid' but found " + found4, "mid", found4);
                        return null;
                    }).isSuccessful());
        } finally {
            shutdown();
        }
    }

    @Test
    public void nestedInnerRetry() throws InterruptedException {
        try {
            final ByteBuffer rootGuid = setRootToNZeroObjs(createConnections(1)[0], 1);
            final CountDownLatch retryLatch = new CountDownLatch(1);
            inParallel(2, (final int tId, final Connection c) -> {
                final RefCap[] rootRefs = awaitRootVersionChange(c, rootGuid, 1);
                final RefCap objRef = rootRefs[0];
                if (tId == 0) {
                    try {
                        retryLatch.await();
                        Thread.sleep(250);
                    } catch (final InterruptedException e) {
                    }
                    c.transact(txn -> {
                        txn.write(objRef, ByteBuffer.wrap("magic".getBytes()));
                        return null;
                    }).getResultOrRethrow();

                } else {
                    c.transact(t0 -> {
                        final ValueRefs vr0 = t0.read(objRef);
                        if (t0.restartNeeded()) {
                            return null;
                        }
                        final String found0 = byteBufferToString(vr0.value, "magic".length());
                        if ("magic".equals(found0)) {
                            return null;
                        } else {
                            return t0.transact(t1 -> {
                                // Even though we've not read root in this inner txn,
                                // retry should still work!
                                retryLatch.countDown();
                                t1.retry();
                                return null;
                            }).getResultOrRethrow();
                        }
                    }).getResultOrRethrow();
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
            c.transact(t0 -> {
                final RefCap rootObj0 = t0.root(rootName);
                assertNotNull(rootObj0);
                final RefCap obj0 = t0.transact(t1 -> {
                    final RefCap obj1 = t1.create(ByteBuffer.wrap("Hello".getBytes()));
                    if (t1.restartNeeded()) {
                        return null;
                    }
                    t1.write(t1.root(rootName), null, obj1);
                    return obj1;
                }).getResultOrRethrow();
                ValueRefs vr0 = t0.read(rootObj0);
                if (t0.restartNeeded()) {
                    return null;
                }
                if (vr0.references.length != 1 || !vr0.references[0].sameReferent(obj0)) {
                    fail("Expected to find obj0 as only ref from root. Instead found " + Arrays.toString(vr0.references));
                }
                final ValueRefs vr1 = t0.read(obj0);
                if (t0.restartNeeded()) {
                    return null;
                }
                final String val0 = byteBufferToString(vr1.value, "Hello".length());
                assertEquals("Expected to find 'Hello' as value of obj0. Instead found " + val0, "Hello", val0);
                t0.write(obj0, ByteBuffer.wrap("Goodbye".getBytes()));
                return null;
            }).getResultOrRethrow();


            final String val1 = c.transact(t0 -> {
                final ValueRefs vr0 = t0.read(t0.root(rootName));
                if (t0.restartNeeded()) {
                    return null;
                }
                final ValueRefs vr1 = t0.read(vr0.references[0]);
                if (t0.restartNeeded()) {
                    return null;
                }
                return byteBufferToString(vr1.value, "Goodbye".length());
            }).getResultOrRethrow();
            assertEquals("Expected to find 'Goodbye' as value of obj0. Instead found " + val1, "Goodbye", val1);
        } finally {
            shutdown();
        }
    }
}
