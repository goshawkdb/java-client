package io.goshawkdb.test;

import org.junit.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.Transaction;

public class NestedTest extends TestBase {

    public NestedTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void nestedRead() throws Throwable {
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
                        throw new IllegalStateException("Expecting to get 43 back from nested txn but got " + r2);
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
}
