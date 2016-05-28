package io.goshawkdb.test;

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;
import io.goshawkdb.client.TransactionResult;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileReader;

import static io.goshawkdb.test.TestBase.getEnv;

/**
 * @author pidster
 */
public class SimpleSetupTest {

    @Test
    public void test() throws Exception {

        final String clusterCertPath = getEnv("CLUSTER_CERT");
        final String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        Certs certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        final String hostStr = getEnv("CLUSTER_HOSTS");
        String[] hosts = hostStr.split(",");

        ConnectionFactory factory = new ConnectionFactory();

        // try-with-resources auto-closes the connection
        try (Connection connection = factory.connect(certs, hosts[0])) {
            TransactionResult<Integer> result = connection.runTransaction(t -> 1);
        }
        catch (Throwable throwable) {
            throwable.printStackTrace();
        }


    }

}
