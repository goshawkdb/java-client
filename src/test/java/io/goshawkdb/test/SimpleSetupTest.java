package io.goshawkdb.test;

import org.junit.Test;

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
import io.goshawkdb.client.TransactionResult;

import static io.goshawkdb.test.TestBase.getEnv;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author pidster
 */
public class SimpleSetupTest {

    @Test
    public void test() throws NoSuchProviderException, NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException, InvalidKeySpecException, InvalidKeyException, InterruptedException {
        final String clusterCertPath = getEnv("CLUSTER_CERT");
        final String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        final Certs certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        final String hostStr = getEnv("CLUSTER_HOSTS");
        final String[] hosts = hostStr.split(",");

        final ConnectionFactory factory = new ConnectionFactory();

        // try-with-resources auto-closes the connection
        try (final Connection c = factory.connect(certs, hosts[0])) {
            TransactionResult<Integer> result = c.transact(t -> 1);
            assertEquals(1, result.result.intValue());
            assertNull(result.cause);
            result.getResultOrRethrow();
        }
    }
}
