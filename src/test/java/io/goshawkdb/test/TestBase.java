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

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;

public class TestBase {

    private final ConnectionFactory factory;
    private final Certs certs;

    private final String[] hosts;

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
            conns[idx] = factory.connect(certs, hosts[idx % hosts.length]);
        }
        return conns;
    }

    protected void shutdownGracefully() {
        factory.group.shutdownGracefully();
    }

}
