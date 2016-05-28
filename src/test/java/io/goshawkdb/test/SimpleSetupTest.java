package io.goshawkdb.test;

/**
 * @author pidster
 */

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileReader;

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;
import io.goshawkdb.client.TransactionResult;

import static io.goshawkdb.test.TestBase.getEnv;

/**
 * @author pidster
 */
public class SimpleSetupTest {

    @Test
    public void test() throws Throwable {

        final String clusterCertPath = getEnv("CLUSTER_CERT");
        final String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        final Certs certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        final String hostStr = getEnv("CLUSTER_HOSTS");
        final String[] hosts = hostStr.split(",");

        final ConnectionFactory factory = new ConnectionFactory();

        // try-with-resources auto-closes the connection
        try (final Connection connection = factory.connect(certs, hosts[0])) {
            final TransactionResult<Integer> result = connection.runTransaction(t -> 1);
        }
    }
}
