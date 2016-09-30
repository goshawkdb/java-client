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

import io.goshawkdb.client.Capability;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObjRef;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

public class CapabilitiesTest extends TestBase {

    public CapabilitiesTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    private void createObjOffRoot(final Connection c, final Capability cap, final ByteBuffer value) {
        runTransaction(c, txn -> {
            final GoshawkObjRef root = getRoot(txn);
            final GoshawkObjRef obj = txn.createObject(value);
            root.set(null, obj.grantCapability(cap));
            return null;
        });
    }

    private void attemptRead(final Connection c, final int refsLen, final int refsIdx, final Capability refCap, final Capability objCap, final ByteBuffer value) {
        final ByteBuffer read = runTransaction(c, txn -> {
            final GoshawkObjRef root = getRoot(txn);
            final GoshawkObjRef[] refs = root.getReferences();
            if (refs.length != refsLen) {
                throw new IllegalStateException("Expected root to have " + refsLen + " reference(s); got " + refs.length);
            }
            final GoshawkObjRef obj = refs[refsIdx];
            if (refCap != obj.getRefCapability()) {
                throw new IllegalStateException("Expected " + refCap + " reference capability; got " + obj.getRefCapability());
            }
            if (objCap != obj.getObjCapability()) {
                throw new IllegalStateException("Expected " + objCap + " object capability; got " + obj.getObjCapability());
            }
            if (objCap.canRead()) {
                return obj.getValue();
            } else {
                try {
                    obj.getValue();
                } catch (IllegalArgumentException e) {
                    return null;
                }
                throw new IllegalStateException("Expected to error on illegal read attempt");
            }
        });
        if (objCap.canRead()) {
            assertEquals("Read the wrong value.", value, read);
        }
    }

    private void attemptWrite(final Connection c, final int refsLen, final int refsIdx, final Capability refCap, final Capability objCap, final ByteBuffer value) {
        runTransaction(c, txn -> {
            final GoshawkObjRef root = getRoot(txn);
            final GoshawkObjRef[] refs = root.getReferences();
            if (refs.length != refsLen) {
                throw new IllegalStateException("Expected root to have " + refsLen + " reference(s); got " + refs.length);
            }
            final GoshawkObjRef obj = refs[refsIdx];
            if (refCap != obj.getRefCapability()) {
                throw new IllegalStateException("Expected " + refCap + " reference capability; got " + obj.getRefCapability());
            }
            if (objCap != obj.getObjCapability()) {
                throw new IllegalStateException("Expected " + objCap + " object capability; got " + obj.getObjCapability());
            }
            if (objCap.canWrite()) {
                obj.set(value);
                return null;
            } else {
                try {
                    obj.set(value);
                } catch (IllegalArgumentException e) {
                    return null;
                }
                throw new IllegalStateException("Expected to error on illegal write attempt");
            }
        });
    }

    @Test
    public void none() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // c1 writes a ref to root with none caps
            createObjOffRoot(c1, Capability.None, ByteBuffer.wrap("Hello World".getBytes()));
            // c2 shouldn't be able to read it
            attemptRead(c2, 1, 0, Capability.None, Capability.None, null);
            // and c2 shouldn't be able to write it
            attemptWrite(c2, 1, 0, Capability.None, Capability.None, ByteBuffer.wrap("illegal".getBytes()));
        } finally {
            shutdown();
        }
    }

    @Test
    public void readOnly() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // c1 writes a ref to root with read caps
            createObjOffRoot(c1, Capability.Read, ByteBuffer.wrap("Hello World".getBytes()));
            // c2 should be able to read it
            attemptRead(c2, 1, 0, Capability.Read, Capability.Read, ByteBuffer.wrap("Hello World".getBytes()));
            // but c2 shouldn't be able to write it
            attemptWrite(c2, 1, 0, Capability.Read, Capability.Read, ByteBuffer.wrap("illegal".getBytes()));
        } finally {
            shutdown();
        }
    }

    @Test
    public void writeOnly() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // c1 writes a ref to root with write caps
            createObjOffRoot(c1, Capability.Write, ByteBuffer.wrap("Hello World".getBytes()));
            // c2 shouldn't be able to read it
            attemptRead(c2, 1, 0, Capability.Write, Capability.Write, null);
            // but c2 should be able to write it
            attemptWrite(c2, 1, 0, Capability.Write, Capability.Write, ByteBuffer.wrap("Goodbye World".getBytes()));
            // and c1 should be able to read it, as it created it, even though
            // it'll only find a Write capability on the ref.
            attemptRead(c1, 1, 0, Capability.Write, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
        } finally {
            shutdown();
        }
    }

    @Test
    public void readWrite() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // c1 writes a ref to root with readWrite caps
            createObjOffRoot(c1, Capability.ReadWrite, ByteBuffer.wrap("Hello World".getBytes()));
            // c2 should be able to read it
            attemptRead(c2, 1, 0, Capability.ReadWrite, Capability.ReadWrite, ByteBuffer.wrap("Hello World".getBytes()));
            // and c2 should be able to write it
            attemptWrite(c2, 1, 0, Capability.ReadWrite, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
            // and c1 should be able to read it, as it created it.
            attemptRead(c1, 1, 0, Capability.ReadWrite, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
        } finally {
            shutdown();
        }
    }

    @Test
    public void fakeRead() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // c1 writes a ref to root with write-only caps
            createObjOffRoot(c1, Capability.Write, ByteBuffer.wrap("Hello World".getBytes()));
            // c2 shouldn't be able to read it
            attemptRead(c2, 1, 0, Capability.Write, Capability.Write, null);
            // and even if we're bad and fake the capability, we shouldn't be
            // able to read it. There is no point faking it locally only as the
            // server hasn't sent c2 the value. So the only hope is to fake it
            // locally and write it back into the root. Of course, the server
            // should reject the txn:
            try {
                runTransaction(c2, txn -> {
                    final GoshawkObjRef root = getRoot(txn);
                    final GoshawkObjRef[] refs = root.getReferences();
                    if (refs.length != 1) {
                        throw new IllegalStateException("Expected root to have 1 reference; got " + refs.length);
                    }
                    // we will only have write on this ref (and on the obj)
                    final GoshawkObjRef obj = refs[0];
                    root.set(null, obj.grantCapability(Capability.Read));
                    return null;
                });
            } catch (final RuntimeException e) {
                return;
            }
            fail("Should have got an exception due to illegal capability widening");
        } finally {
            shutdown();
        }
    }

    @Test
    public void fakeWrite() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // c1 writes a ref to root with read-only caps
            createObjOffRoot(c1, Capability.Read, ByteBuffer.wrap("Hello World".getBytes()));
            // c2 shouldn't be able to write it
            attemptWrite(c2, 1, 0, Capability.Read, Capability.Read, ByteBuffer.wrap("illegal".getBytes()));
            // and even if we're bad and fake the capability, we shouldn't be
            // able to write it. There is no point faking it locally only as the
            // server hasn't sent c2 the value. So the only hope is to fake it
            // locally and write it back into the root. Of course, the server
            // should reject the txn:
            try {
                runTransaction(c2, txn -> {
                    final GoshawkObjRef root = getRoot(txn);
                    final GoshawkObjRef[] refs = root.getReferences();
                    if (refs.length != 1) {
                        throw new IllegalStateException("Expected root to have 1 reference; got " + refs.length);
                    }
                    // we will only have read on this ref (and on the obj)
                    final GoshawkObjRef obj = refs[0];
                    root.set(null, obj.grantCapability(Capability.Write));
                    return null;
                });
            } catch (final RuntimeException e) {
                return;
            }
            fail("Should have got an exception due to illegal capability widening");
        } finally {
            shutdown();
        }
    }

    @Test
    public void capabilitiesCanGrowSingleTxn() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];

            // we want to construct the following graph
            // root --rw--> obj3 --rw--> obj2
            //     1\   0   1|r      0     0|
            //       \       v              /
            //        \-n-> obj1 <--w------/
            //
            // However, because we're creating this whole structure in a single
            // txn, c2 will get told about the whole txn in one go, and so
            // should immediately learn that obj1 is read-write.

            runTransaction(c1, txn -> {
                final GoshawkObjRef root = getRoot(txn);
                final GoshawkObjRef obj1 = txn.createObject(ByteBuffer.wrap("Hello World".getBytes()));
                final GoshawkObjRef obj2 = txn.createObject(null, obj1.grantCapability(Capability.Write));
                final GoshawkObjRef obj3 = txn.createObject(null, obj2, obj1.grantCapability(Capability.Read));
                root.set(null, obj3, obj1.grantCapability(Capability.None));
                return null;
            });
            attemptRead(c2, 2, 1, Capability.None, Capability.ReadWrite, ByteBuffer.wrap("Hello World".getBytes()));
            attemptWrite(c2, 2, 1, Capability.None, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
            attemptRead(c1, 2, 1, Capability.None, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
        } finally {
            shutdown();
        }
    }

    @Test
    public void capabilitiesCanGrowMultiTxn() throws InterruptedException {
        try {
            final Connection[] conns = createConnections(2);
            final Connection c1 = conns[0];
            final Connection c2 = conns[1];
            // we want to construct the same graph as last time:
            // root --rw--> obj3 --rw--> obj2
            //     1\   0   1|r      0     0|
            //       \       v              /
            //        \-n-> obj1 <--w------/
            //
            // This time though we do it in multiple txns which means c2 will
            // actually have to read bits to finally discover its full
            // capabilities on obj1: The point is that when c2 only reaches
            // root, it should have no access to obj1.  After it's reached obj3
            // it should be able to read only obj1. After it's reached obj2, it
            // should have read-write access to obj1.

            // txn1: create all the objs, but only have root point to obj1.
            runTransaction(c1, txn -> {
                final GoshawkObjRef root = getRoot(txn);
                final GoshawkObjRef obj1 = txn.createObject(ByteBuffer.wrap("Hello World".getBytes()));
                final GoshawkObjRef obj2 = txn.createObject(null);
                final GoshawkObjRef obj3 = txn.createObject(null, obj2);
                root.set(null, obj3, obj1.grantCapability(Capability.None));
                return null;
            });
            // txn2: add the read pointer from obj3 to obj1
            runTransaction(c1, txn -> {
                final GoshawkObjRef root = getRoot(txn);
                final GoshawkObjRef[] rootRefs = root.getReferences();
                final GoshawkObjRef obj3 = rootRefs[0];
                final GoshawkObjRef obj1 = rootRefs[1];
                final GoshawkObjRef[] obj3Refs = obj3.getReferences();
                obj3.set(null, obj3Refs[0], obj1.grantCapability(Capability.Read));
                return null;
            });
            // txn3: add the write pointer from obj2 to obj1
            runTransaction(c1, txn -> {
                final GoshawkObjRef root = getRoot(txn);
                final GoshawkObjRef[] rootRefs = root.getReferences();
                final GoshawkObjRef obj3 = rootRefs[0];
                final GoshawkObjRef obj1 = rootRefs[1];
                final GoshawkObjRef[] obj3Refs = obj3.getReferences();
                final GoshawkObjRef obj2 = obj3Refs[0];
                obj2.set(null, obj1.grantCapability(Capability.Write));
                return null;
            });
            // initially, c2 should not be able to read obj1
            attemptRead(c2, 2, 1, Capability.None, Capability.None, null);
            // but, if c2 first reads obj3, it should find it can read obj1
            attemptRead(c2, 2, 0, Capability.ReadWrite, Capability.ReadWrite, ByteBuffer.wrap(new byte[]{}));
            attemptRead(c2, 2, 1, Capability.None, Capability.Read, ByteBuffer.wrap("Hello World".getBytes()));

            // finally, if c2 reads to obj2 then we should discover we can actually write obj1
            runTransaction(c2, txn -> {
                final GoshawkObjRef root = getRoot(txn);
                final GoshawkObjRef[] rootRefs = root.getReferences();
                final GoshawkObjRef obj3 = rootRefs[0];
                final GoshawkObjRef[] obj3Refs = obj3.getReferences();
                final GoshawkObjRef obj2 = obj3Refs[0];
                return obj2.getValue();
            });
            attemptWrite(c2, 2, 1, Capability.None, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
            attemptRead(c1, 2, 1, Capability.None, Capability.ReadWrite, ByteBuffer.wrap("Goodbye World".getBytes()));
        } finally {
            shutdown();
        }
    }
}
