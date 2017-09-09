package io.goshawkdb.test;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;
import io.goshawkdb.client.RefCap;
import io.goshawkdb.client.ValueRefs;

import static junit.framework.TestCase.assertNotNull;

public class TestBase {

    public interface ParRunner {
        void run(final int parIndex, final Connection conn) throws Exception;
    }

    private final ConnectionFactory factory;
    private final Certs certs;
    private final String[] hosts;
    private final List<Connection> connections = new ArrayList<>();
    private final Random rng = new Random();
    public final String rootName;

    protected TestBase() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        final String clusterCertPath = getEnv("CLUSTER_CERT");
        final String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        final String hostStr = getEnv("CLUSTER_HOSTS");
        hosts = hostStr.split(",");

        rootName = getEnv("ROOT_NAME");

        factory = new ConnectionFactory();
    }

    protected static String getEnv(final String suffix) {
        String result = System.getenv("GOSHAWKDB_" + suffix);
        if (result == null) {
            result = System.getenv("GOSHAWKDB_DEFAULT_" + suffix);
            if (result == null) {
                throw new IllegalArgumentException("No GOSHAWKDB_" + suffix + " env var defined");
            }
        }
        return result;
    }

    protected Connection[] createConnections(final int n) throws InterruptedException {
        final Connection[] conns = new Connection[n];
        for (int idx = 0; idx < n; idx++) {
            final Connection c = factory.connect(certs, hosts[idx % hosts.length]);
            assertNotNull(c);
            conns[idx] = c;
            connections.add(c);
        }
        return conns;
    }

    protected void inParallel(final int parCount, final ParRunner runner) throws InterruptedException {
        final ConcurrentLinkedDeque<RuntimeException> exceptionQueue = new ConcurrentLinkedDeque<>();
        final Connection[] conns = createConnections(parCount);
        final Thread[] threads = new Thread[parCount];
        for (int idx = 0; idx < parCount; idx++) {
            final int idxCopy = idx;
            final Connection conn = conns[idxCopy];
            threads[idx] = new Thread(() -> {
                try {
                    runner.run(idxCopy, conn);
                } catch (final RuntimeException e) {
                    exceptionQueue.add(e);
                } catch (final Exception e) {
                    exceptionQueue.add(new RuntimeException(e));
                }
            });
        }
        for (final Thread t : threads) {
            t.start();
        }
        for (final Thread t : threads) {
            t.join();
        }
        final RuntimeException e = exceptionQueue.peek();
        if (e != null) {
            throw e;
        }
    }

    /**
     * Creates n objects, each with 8 0-bytes as their value, and links to all of them from the root
     * object, which has an empty value set.
     */
    protected ByteBuffer setRootToNZeroObjs(final Connection c, final int n) {
        final byte[] bytes = new byte[8];
        rng.nextBytes(bytes);
        final ByteBuffer value = ByteBuffer.wrap(bytes);
        c.transact(txn -> {
            final RefCap[] objs = new RefCap[n];
            for (int idx = 0; idx < n; idx++) {
                objs[idx] = txn.create(ByteBuffer.allocate(8));
                if (txn.restartNeeded()) {
                    return null;
                }
            }
            final RefCap root = txn.root(rootName);
            if (root == null) {
                throw new IllegalArgumentException("No such root: " + rootName);
            }
            txn.write(root, value, objs);
            return null;
        }).getResultOrRethrow();
        return value;
    }

    protected RefCap[] awaitRootVersionChange(final Connection c, final ByteBuffer value, final int refsCount) {
        return c.transact(txn -> {
            final RefCap root = txn.root(rootName);
            if (root == null) {
                throw new IllegalArgumentException("No such root: " + rootName);
            } else {
                final ValueRefs vr = txn.read(root);
                if (txn.restartNeeded()) {
                    return null;
                }
                if (!vr.value.equals(value) || vr.references.length != refsCount) {
                    txn.retry();
                    return null;
                } else {
                    return vr.references;
                }
            }
        }).getResultOrRethrow();
    }

    protected void shutdown() throws InterruptedException {
        for (final Iterator<Connection> it = connections.iterator(); it.hasNext(); ) {
            final Connection conn = it.next();
            if (conn != null) {
                conn.close();
            }
        }
        connections.clear();
        factory.close();
    }

    protected static String byteBufferToString(final ByteBuffer buf, final int len) {
        final byte[] ary = new byte[len];
        buf.get(ary);
        return new String(ary);
    }
}
