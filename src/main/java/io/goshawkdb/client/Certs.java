package io.goshawkdb.client;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Class for managing the cluster ECDSA certificate and public key, and the client ECDSA certificate
 * and key pair. GoshawkDB only speaks TLSv1.2 on TCP, and only uses ECDSA keys on P256.
 */
public class Certs {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    // The only cipher that GoshawkDB speaks.
    public static final String CIPHER = "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256";

    private final KeyFactory keyFactory;

    // KeyStore is for the cluster certificate only.
    private KeyStore keyStore;

    // These are for the client to authenticate against the server.
    private X509CertificateHolder clientCertificateHolder;
    private X509Certificate clientCertificate;
    private PEMKeyPair clientKeyPair;
    private PrivateKey clientPrivateKey;

    public Certs() throws NoSuchProviderException, NoSuchAlgorithmException {
        keyFactory = KeyFactory.getInstance("ECDSA", "BC");
    }

    private Certs ensureKeyStore() throws CertificateException, NoSuchAlgorithmException, IOException, KeyStoreException {
        if (keyStore == null) {
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
        }
        return this;
    }

    /**
     * Provided in case you wish to provide your own keystore (for example one that is stored on
     * disk rather than an ephemeral one). The keystore is only used to hold the cluster
     * certificate, and thus validate the certificate presented by the GoshawkDB node to which you
     * connect (as opposed to the client certificate and key pair. I.e. even if you set a key store,
     * you still need to call setClientCertificateHolder and setClientKeyPair, or parseClientPEM).
     * If you supply your own KeyStore, you must have initialized it yourself.
     */
    public Certs setKeyStore(final KeyStore ks) {
        keyStore = ks;
        return this;
    }

    private TrustManagerFactory getTrustManagerFactory() throws NoSuchAlgorithmException, KeyStoreException {
        if (keyStore == null) {
            return InsecureTrustManagerFactory.INSTANCE;
        } else {
            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
    }

    /**
     * Loads a single X.509 certificate from the provided InputStream into the current KeyStore. If
     * no KeyStore has been set, certificates will be loaded into a fresh ephemeral KeyStore. Will
     * always close the InputStream.
     */
    public Certs addClusterCertificate(final String alias, final InputStream is) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        try {
            CertificateFactory clusterCertFact = CertificateFactory.getInstance("X.509");
            Certificate clusterCert = clusterCertFact.generateCertificate(is);
            ensureKeyStore();
            keyStore.setCertificateEntry(alias, clusterCert);
            return this;
        } finally {
            is.close();
        }
    }

    public Certs addClusterCertificate(final String alias, final byte[] cert) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        return addClusterCertificate(alias, new ByteArrayInputStream(cert));
    }

    /**
     * Set the ClientCertificateHolder. This is the X.509 certificate and public key the client will
     * present to the GoshawkDB node for authentication. The public key must be an ECDSA P256 key.
     * Once the ClientCertificateHolder and the ClientKeyPair are both set, it is verified that they
     * both contain the same public key.
     */
    public Certs setClientCertificateHolder(final X509CertificateHolder certHolder) throws CertificateException, InvalidKeySpecException, InvalidKeyException, IOException {
        clientCertificateHolder = certHolder;
        clientCertificate = null;
        verifyClient();
        return this;
    }

    /**
     * Set the ClientKeyPair. This must be an ECDSA P256 key pair in PEM format. Once the
     * ClientCertificateHolder and the ClientKeyPair are both set, it is verified that they both
     * contain the same public key.
     */
    public Certs setClientKeyPair(final PEMKeyPair keyPair) throws CertificateException, InvalidKeySpecException, InvalidKeyException, IOException {
        clientKeyPair = keyPair;
        clientPrivateKey = null;
        verifyClient();
        return this;
    }

    /**
     * Parse the contents of the provided Reader for an X.509 Certificate with public key, and a PEM
     * Key Pair, and calls setClientCertificateHolder and setClientKeyPair as appropriate. Only the
     * first X.509 Certificate and the first PEM Key Pair are read, but order within the Reader does
     * not matter. The Reader is always closed.
     */
    public Certs parseClientPEM(final Reader reader) throws IOException, CertificateException, InvalidKeySpecException, InvalidKeyException {
        clientKeyPair = null;
        clientPrivateKey = null;
        clientCertificateHolder = null;
        clientCertificate = null;
        final PEMParser parser = new PEMParser(reader);
        try {
            Object o = null;
            boolean foundCert = false;
            boolean foundKeyPair = false;
            while ((o = parser.readObject()) != null && !(foundCert && foundKeyPair)) {
                if (o instanceof X509CertificateHolder) {
                    setClientCertificateHolder((X509CertificateHolder) o);
                    foundCert = true;
                }
                if (o instanceof PEMKeyPair) {
                    setClientKeyPair((PEMKeyPair) o);
                    foundKeyPair = true;
                }
            }
        } finally {
            parser.close();
        }
        return this;
    }

    private void verifyClient() throws CertificateException, IOException, InvalidKeySpecException, InvalidKeyException {
        SubjectPublicKeyInfo clientCertificatePubKeyInfo = null;
        SubjectPublicKeyInfo clientKeyPairPubKeyInfo = null;

        if (clientCertificateHolder != null) {
            clientCertificatePubKeyInfo = clientCertificateHolder.getSubjectPublicKeyInfo();
            if (!X9ObjectIdentifiers.id_ecPublicKey.equals(clientCertificatePubKeyInfo.getAlgorithm().getAlgorithm())) {
                clientCertificateHolder = null;
                throw new CertificateException("ClientCertificateHolder must contain an EC public key");
            }
        }

        if (clientKeyPair != null) {
            clientKeyPairPubKeyInfo = clientKeyPair.getPublicKeyInfo();
            if (!X9ObjectIdentifiers.id_ecPublicKey.equals(clientKeyPairPubKeyInfo.getAlgorithm().getAlgorithm())) {
                clientKeyPair = null;
                throw new InvalidKeyException("ClientKeyPair's Public Key must be an EC public key");
            }
        }

        if (clientCertificateHolder != null && clientKeyPair != null) {
            final PublicKey clientCertificatePublicKey = keyFactory.generatePublic(new X509EncodedKeySpec(clientCertificatePubKeyInfo.getEncoded()));
            if (!(clientCertificatePublicKey instanceof BCECPublicKey)) {
                clientCertificateHolder = null;
                throw new CertificateException("ClientCertificateHolder must contain an EC public key");
            }
            final ECPoint clientCertificatePublicKeyQ = ((BCECPublicKey) clientCertificatePublicKey).getQ();

            final PublicKey clientKeyPairPublicKey = keyFactory.generatePublic(new X509EncodedKeySpec(clientKeyPairPubKeyInfo.getEncoded()));
            if (!(clientKeyPairPublicKey instanceof BCECPublicKey)) {
                clientKeyPair = null;
                throw new InvalidKeyException("ClientKeyPair must contain an EC public key");
            }
            final ECPoint clientKeyPairPublicKeyQ = ((BCECPublicKey) clientKeyPairPublicKey).getQ();

            if (!clientCertificatePublicKeyQ.equals(clientKeyPairPublicKeyQ)) {
                clientCertificateHolder = null;
                clientKeyPair = null;
                throw new InvalidKeyException("ClientKeyPair's public key does not match the public key in ClientCertificateHolder");
            }
            clientPrivateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(clientKeyPair.getPrivateKeyInfo().getEncoded()));
            clientCertificate = new JcaX509CertificateConverter().setProvider("BC").getCertificate(clientCertificateHolder);
        }
    }

    /**
     * Builds a netty SslContext for clients using the KeyStore, ClientCertificateHolder and
     * ClientKeyPair. A new TrustManagerFactory will be created and initialised with the current
     * KeyStore. If no KeyStore and no ClusterCertificate have been provided then an
     * InsecureTrustManagerFactory will be used which will accept ANY certificate supplied by the
     * GoshawkDB node: this is insecure - DO NOT USE THIS IN PRODUCTION. The ClientCertificateHolder
     * and ClientKeyPair must have been set, either by calling them directly, or through calling
     * parseClientPEM.
     */
    public SslContext buildClientSslContext() throws KeyStoreException, NoSuchAlgorithmException, CertificateException, SSLException {
        if (clientCertificateHolder == null || clientKeyPair == null) {
            throw new IllegalStateException("ClientCertificateHolder and ClientKeyPair must be provided");
        }
        return SslContextBuilder.forClient()
                .sslProvider(SslProvider.JDK)
                .trustManager(getTrustManagerFactory())
                .ciphers(Arrays.asList(CIPHER))
                .keyManager(clientPrivateKey, clientCertificate)
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .build();
    }
}
