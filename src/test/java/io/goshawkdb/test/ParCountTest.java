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
import java.util.concurrent.ConcurrentLinkedDeque;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.VarUUId;

public class ParCountTest extends TestBase {

    public ParCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void test() throws Throwable {
        final int threadCount = 8;
        final Connection conn = factory.connect(certs, "localhost", 10001);
        final ArrayList<GoshawkObj> roots = conn.runTransaction((txn) -> {
            final ArrayList<GoshawkObj> objs = new ArrayList<>(threadCount);
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
                objs.add(txn.createObject(ByteBuffer.allocate(0)));
            }
            final GoshawkObj root = txn.getRoot();
            root.setReferences(objs.toArray(new GoshawkObj[threadCount]));
            return objs;
        });
        conn.close();

        final ConcurrentLinkedDeque<Throwable> exceptionQueue = new ConcurrentLinkedDeque<>();

        final Thread[] threads = new Thread[threadCount];
        for (int threadIndex = 0; threadIndex < threads.length; threadIndex++) {
            final int threadIndexCopy = threadIndex;
            final Thread t = new Thread(() -> {
                try {
                    final Connection c = factory.connect(certs, "localhost", 10001);
                    final VarUUId rootId = c.runTransaction(txn ->
                            txn.getRoot().getReferences()[threadIndexCopy].id
                    );
                    final long start = System.nanoTime();
                    long expected = 0L;
                    for (int idx = 0; idx < 1000; idx++) {
                        final int idy = idx;
                        final long expectedCopy = expected;
                        expected = c.runTransaction((txn) -> {
                            final GoshawkObj root = txn.getObject(rootId);
                            if (idy == 0) {
                                root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0));
                                return 0L;
                            } else {
                                final long old = root.getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                                if (old == expectedCopy) {
                                    final long val = old + 1;
                                    root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0, val));
                                    return val;
                                } else {
                                    throw new IllegalStateException("" + threadIndexCopy + ": Expected " + expectedCopy + " but found " + old);
                                }
                            }
                        });
                    }
                    final long end = System.nanoTime();
                    System.out.println("" + threadIndexCopy + ": Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
                    c.close();
                } catch (Throwable e) {
                    exceptionQueue.add(e);
                } finally {
                    try {
                        conn.close();
                    } catch (InterruptedException e) {
                        exceptionQueue.add(e);
                    }
                }
            });
            threads[threadIndex] = t;
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        final Throwable t = exceptionQueue.peek();
        if (t != null) {
            throw t;
        }
        factory.group.shutdownGracefully();
    }
}
