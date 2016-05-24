package io.goshawkdb.test;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
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

public class TestBase {

    public interface ParRunner {
        void run(final int parIndex, final Connection conn, final Queue<Throwable> exceptionQ) throws Throwable;
    }

    private final ConnectionFactory factory;
    private final Certs certs;
    private final String[] hosts;
    private final List<Connection> connections = new ArrayList<>();

    TestBase() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        final String clusterCertPath = getEnv("CLUSTER_CERT");
        final String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        final String hostStr = getEnv("CLUSTER_HOSTS");
        hosts = hostStr.split(",");

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
            conns[idx] = c;
            connections.add(c);
        }
        return conns;
    }

    protected void inParallel(final int parCount, final ParRunner runner) throws Throwable {
        final ConcurrentLinkedDeque<Throwable> exceptionQueue = new ConcurrentLinkedDeque<>();
        final Connection[] conns = createConnections(parCount);
        final Thread[] threads = new Thread[parCount];
        for (int idx = 0; idx < parCount; idx++) {
            final int idxCopy = idx;
            threads[idx] = new Thread(() -> {
                final Connection conn = conns[idxCopy];
                try {
                    runner.run(idxCopy, conn, exceptionQueue);
                } catch (final Throwable t) {
                    exceptionQueue.add(t);
                }
            });
        }
        for (final Thread t : threads) {
            t.start();
        }
        for (final Thread t : threads) {
            t.join();
        }
        final Throwable t = exceptionQueue.peek();
        if (t != null) {
            throw t;
        }
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
