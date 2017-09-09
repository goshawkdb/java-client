package io.goshawkdb.client;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Class for managing the cluster ECDSA certificate and public key, and the client ECDSA certificate
 * and key pair. GoshawkDB only speaks TLSv1.2 on TCP, and only uses ECDSA keys on P256.
 */
public class Certs {

    static {
        Security.addProvider(new BouncyCastleProvider());
        Security.addProvider(new BouncyCastleJsseProvider());
    }

    // The only cipher that GoshawkDB speaks.
    public static final String[] CIPHERS = new String[]{"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"};

    private final KeyFactory keyFactory;

    // KeyStore is for the cluster certificate only.
    private KeyStore keyStore;

    // These are for the client to authenticate against the server.
    private X509CertificateHolder clientCertificateHolder;
    private X509Certificate clientCertificate;
    private PEMKeyPair clientKeyPair;
    private PrivateKey clientPrivateKey;

    /**
     * Create a new Certs.
     *
     * @throws NoSuchProviderException  if BouncyCastle can't be found
     * @throws NoSuchAlgorithmException if ECDSA support can't be found
     */
    public Certs() throws NoSuchProviderException, NoSuchAlgorithmException {
        keyFactory = KeyFactory.getInstance("ECDSA", BouncyCastleProvider.PROVIDER_NAME);
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
     *
     * @param ks The KeyStore to use.
     * @return A new Certs
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
     *
     * @param alias The name under which to store the certificate (just pick something sane)
     * @param is    The InputStream to read the certificate from.
     * @return The Certs object for method chaining
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

    /**
     * Helper in case the cluster certificate is already loaded is a byte[]
     *
     * @param alias The name under which to store the certificate (just pick something sane)
     * @param cert  The bytes of the certificate
     * @return The Certs object for method chaining
     */
    public Certs addClusterCertificate(final String alias, final byte[] cert) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        return addClusterCertificate(alias, new ByteArrayInputStream(cert));
    }

    /**
     * Set the ClientCertificateHolder. This is the X.509 certificate and public key the client will
     * present to the GoshawkDB node for authentication. The public key must be an ECDSA P256 key.
     * Once the ClientCertificateHolder and the ClientKeyPair are both set, it is verified that they
     * both contain the same public key.
     *
     * @param certHolder The holder for the client certificate and public key
     * @return The Certs object for method chaining
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
     *
     * @param keyPair The client public and private key pair
     * @return The Certs object for method chaining
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
     *
     * @param reader The reader to read from
     * @return The Certs object for method chaining
     */
    public Certs parseClientPEM(final Reader reader) throws CertificateException, InvalidKeySpecException, InvalidKeyException, IOException {
        try (final PEMParser parser = new PEMParser(reader)) {
            Object o;
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
        }
        return this;
    }

    private void verifyClient() throws CertificateException, InvalidKeyException, IOException, InvalidKeySpecException {
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
            clientCertificate = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(clientCertificateHolder);
        }
    }

    SslContext buildClientSslContext() throws KeyStoreException, NoSuchAlgorithmException, SSLException {
        if (clientCertificateHolder == null || clientKeyPair == null) {
            throw new IllegalStateException("ClientCertificateHolder and ClientKeyPair must be provided");
        }

        return SslContextBuilder.forClient()
                .sslContextProvider(new BouncyCastleJsseProvider())
                .protocols("TLSv1.2")
                .trustManager(getTrustManagerFactory())
                .ciphers(Arrays.asList(CIPHERS))
                .keyManager(clientPrivateKey, clientCertificate)
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .build();
    }
}
