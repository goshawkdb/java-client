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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;
import io.goshawkdb.client.GoshawkObjRef;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TxnId;

import static junit.framework.TestCase.assertNotNull;

public class TestBase {

    public interface ParRunner {
        void run(final int parIndex, final Connection conn, final Queue<Exception> exceptionQ) throws Exception;
    }

    private final ConnectionFactory factory;
    private final Certs certs;
    private final String[] hosts;
    private final List<Connection> connections = new ArrayList<>();
    private final String rootName;

    TestBase() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
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

    protected void inParallel(final int parCount, final ParRunner runner) throws Exception {
        final ConcurrentLinkedDeque<Exception> exceptionQueue = new ConcurrentLinkedDeque<>();
        final Connection[] conns = createConnections(parCount);
        final Thread[] threads = new Thread[parCount];
        for (int idx = 0; idx < parCount; idx++) {
            final int idxCopy = idx;
            final Connection conn = conns[idxCopy];
            threads[idx] = new Thread(() -> {
                try {
                    runner.run(idxCopy, conn, exceptionQueue);
                } catch (final Exception e) {
                    exceptionQueue.add(e);
                }
            });
        }
        for (final Thread t : threads) {
            t.start();
        }
        for (final Thread t : threads) {
            t.join();
        }
        final Exception e = exceptionQueue.peek();
        if (e != null) {
            throw e;
        }
    }

    protected GoshawkObjRef getRoot(final Transaction txn) {
        final GoshawkObjRef root = txn.getRoots().get(rootName);
        if (root == null) {
            throw new IllegalStateException("No root named '" + rootName + "' is available");
        }
        return root;
    }

    /**
     * Sets the root object to 8 0-bytes, with no references.
     */
    protected TxnId setRootToZeroInt64(final Connection c) {
        return c.runTransaction(txn -> {
            final GoshawkObjRef root = getRoot(txn);
            root.set(ByteBuffer.allocate(8));
            return root.getVersion();
        }).result;
    }

    /**
     * Creates n objects, each with 8 0-bytes as their value, and links to all of them from the root
     * object, which has an empty value set.
     */
    protected TxnId setRootToNZeroObjs(final Connection c, final int n) {
        return c.runTransaction(txn -> {
            final GoshawkObjRef[] objs = new GoshawkObjRef[n];
            for (int idx = 0; idx < n; idx++) {
                objs[idx] = txn.createObject(ByteBuffer.allocate(8));
            }
            final GoshawkObjRef root = getRoot(txn);
            root.set(ByteBuffer.allocate(0), objs);
            return root.getVersion();
        }).result;
    }

    protected TxnId awaitRootVersionChange(final Connection c, final TxnId oldVsn) {
        return c.runTransaction(txn -> {
            final GoshawkObjRef root = getRoot(txn);
            if (root.getVersion().equals(oldVsn)) {
                txn.retry();
            }
            return null;
        }).txnid;
    }

    protected void shutdown() throws InterruptedException {
        for (final Iterator<Connection> it = connections.iterator(); it.hasNext(); ) {
            final Connection conn = it.next();
            if (conn != null) {
                conn.close();
            }
        }
        connections.clear();
        factory.group.shutdownGracefully();
    }

}
