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
        System.out.println(roots);

        final Thread[] threads = new Thread[threadCount];
        for (int threadIndex = 0; threadIndex < threads.length; threadIndex++) {
            final int threadIndexCopy = threadIndex;
            final Thread t = new Thread(() -> {
                try {
                    final Connection c = factory.connect(certs, "localhost", 10001);
                    //Thread.sleep(1000);
                    final VarUUId rootId = c.runTransaction((txn) -> {
                        final GoshawkObj[] refs = txn.getRoot().getReferences();
                        System.out.println("" + refs.length + "[" + threadIndexCopy + "] = " + refs[threadIndexCopy].id);
                        return refs[threadIndexCopy].id;
                    });
                    System.out.println("" + threadIndexCopy + " = " + rootId);
                    final long start = System.nanoTime();
                    for (int idx = 0; idx < 1000; idx++) {
                        final int idy = idx;
                        long result = c.runTransaction((txn) -> {
                            final GoshawkObj root = txn.getObject(rootId);
                            if (idy == 0) {
                                root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0));
                                return 0L;
                            } else {
                                final long val = 1 + root.getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                                root.set(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0, val));
                                return val;
                            }
                        });
                    }
                    final long end = System.nanoTime();
                    System.out.println("" + threadIndexCopy + ": Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
                    c.close();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
            threads[threadIndex] = t;
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        factory.group.shutdownGracefully();
    }
}
