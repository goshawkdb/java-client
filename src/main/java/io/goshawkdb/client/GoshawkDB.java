package io.goshawkdb.client;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class GoshawkDB {

    private static final String clusterCertStr = "-----BEGIN CERTIFICATE-----\n" +
            "MIIBxzCCAW2gAwIBAgIIQqu37k6KPOIwCgYIKoZIzj0EAwIwOjESMBAGA1UEChMJ\n" +
            "R29zaGF3a0RCMSQwIgYDVQQDExtDbHVzdGVyIENBIFJvb3QgQ2VydGlmaWNhdGUw\n" +
            "IBcNMTYwMTAzMDkwODE2WhgPMjIxNjAxMDMwOTA4MTZaMDoxEjAQBgNVBAoTCUdv\n" +
            "c2hhd2tEQjEkMCIGA1UEAxMbQ2x1c3RlciBDQSBSb290IENlcnRpZmljYXRlMFkw\n" +
            "EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEjHBXt+0n477zVZHTsGgu9rLYzNz/WMLm\n" +
            "l7/KC5v2nx+RC9yfkyfBKq8jJk3KYoB/YJ7s8BH0T456/+nRQIUo7qNbMFkwDgYD\n" +
            "VR0PAQH/BAQDAgIEMA8GA1UdEwEB/wQFMAMBAf8wGQYDVR0OBBIEEL9sxrcr6QTw\n" +
            "wk5csm2ZcfgwGwYDVR0jBBQwEoAQv2zGtyvpBPDCTlyybZlx+DAKBggqhkjOPQQD\n" +
            "AgNIADBFAiAy9NW3zE1ACYDWcp+qeTjQOfEtED3c/LKIXhrbzg2N/QIhANLb4crz\n" +
            "9ENxIifhZcJ/S2lqf49xZZS91dLF4x5ApKci\n" +
            "-----END CERTIFICATE-----";

    private static final String clientCertKeyStr = "-----BEGIN CERTIFICATE-----\n" +
            "MIIBszCCAVmgAwIBAgIIfOmxD9dF8ZMwCgYIKoZIzj0EAwIwOjESMBAGA1UEChMJ\n" +
            "R29zaGF3a0RCMSQwIgYDVQQDExtDbHVzdGVyIENBIFJvb3QgQ2VydGlmaWNhdGUw\n" +
            "IBcNMTYwMTAzMDkwODUwWhgPMjIxNjAxMDMwOTA4NTBaMBQxEjAQBgNVBAoTCUdv\n" +
            "c2hhd2tEQjBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABFrAPcdlw5DWQmS9mCFX\n" +
            "FlD6R8ABaBf4LA821wVmPa9tiM6n8vRJvbmHuSjy8LwJJRRjo9GJq7KD6ZmsK9P9\n" +
            "sXijbTBrMA4GA1UdDwEB/wQEAwIHgDATBgNVHSUEDDAKBggrBgEFBQcDAjAMBgNV\n" +
            "HRMBAf8EAjAAMBkGA1UdDgQSBBBX9qcbG4ofUoUTHGwOgGvFMBsGA1UdIwQUMBKA\n" +
            "EL9sxrcr6QTwwk5csm2ZcfgwCgYIKoZIzj0EAwIDSAAwRQIgOK9PVJt7KdvDU/9v\n" +
            "z9gQI8JnVLZm+6gsh6ro9WnaZ8YCIQDXhjfQAWaUmJNTgKq3rLHiEbPS4Mxl7h7S\n" +
            "kbkX/2GIjg==\n" +
            "-----END CERTIFICATE-----\n" +
            "-----BEGIN EC PRIVATE KEY-----\n" +
            "MHcCAQEEIN9Mf6CzDgCs1EbzJqDK3+12wcr7Ua3Huz6qNhyXCrS1oAoGCCqGSM49\n" +
            "AwEHoUQDQgAEWsA9x2XDkNZCZL2YIVcWUPpHwAFoF/gsDzbXBWY9r22Izqfy9Em9\n" +
            "uYe5KPLwvAklFGOj0YmrsoPpmawr0/2xeA==\n" +
            "-----END EC PRIVATE KEY-----";

    public static void main(String[] args) throws Throwable {

        final Certs certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new ByteArrayInputStream(clusterCertStr.getBytes()));
        certs.parseClientPEM(new StringReader(clientCertKeyStr));

        final ConnectionFactory connFactory = new ConnectionFactory();
        try {
            Connection conn = connFactory.connect(certs, "localhost", 10001);
            System.out.println("Connected");

            final long start = System.nanoTime();
            for (int idx = 0; idx < 10000; idx++) {
                final int idy = idx;
                long result = conn.runTransaction((txn) -> {
                    final GoshawkObj root = txn.getRoot();
                    if (idy == 0) {
                        root.set(ByteBuffer.wrap(new byte[8]));
                        return 0L;
                    } else {
                        final long val = 1 + root.getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                        root.set(ByteBuffer.wrap(new byte[8]).order(ByteOrder.BIG_ENDIAN).putLong(0, val));
                        return val;
                    }
                });
                System.out.println(result);
            }
            final long end = System.nanoTime();
            System.out.println("Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
            conn.close();
            conn.awaitClose();
            System.out.println("Disconnected");

        } finally {
            System.out.println("Shutting down...");
            connFactory.group.shutdownGracefully();
            ConnectionFactory.timer.stop();
            System.out.println("...done.");
        }
    }
}
