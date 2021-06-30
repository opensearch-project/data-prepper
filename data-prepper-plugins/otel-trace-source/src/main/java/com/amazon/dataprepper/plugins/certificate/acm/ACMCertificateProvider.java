package com.amazon.dataprepper.plugins.certificate.acm;

import com.amazon.dataprepper.plugins.certificate.model.Certificate;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.certificatemanager.AWSCertificateManager;
import com.amazonaws.services.certificatemanager.AWSCertificateManagerClientBuilder;
import com.amazonaws.services.certificatemanager.model.*;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEInputDecryptorProviderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.security.PrivateKey;
import java.security.Security;

public class ACMCertificateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ACMCertificateProvider.class);
    private static final long SLEEP_INTERVAL = 10000L;
    private static final String BOUNCY_CASTLE_PROVIDER = "BC";
    private final AWSCertificateManager awsCertificateManager;
    private final long totalTimeout;

    public ACMCertificateProvider(final String awsRegion, final long totalTimeout) {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final ClientConfiguration clientConfig = new ClientConfiguration()
                .withThrottledRetries(true);
        this.awsCertificateManager = AWSCertificateManagerClientBuilder.standard()
            .withRegion(awsRegion)
            .withCredentials(credentialsProvider)
            .withClientConfiguration(clientConfig)
            .build();
        this.totalTimeout = totalTimeout;
        Security.addProvider(new BouncyCastleProvider());
    }

    // accessible only in the same package for unit test
    ACMCertificateProvider(final AWSCertificateManager awsCertificateManager, final long totalTimeout) {
        this.awsCertificateManager = awsCertificateManager;
        this.totalTimeout = totalTimeout;
        Security.addProvider(new BouncyCastleProvider());
    }

    public Certificate getACMCertificate(final String acmArn, final String passphrase) {
        ExportCertificateResult exportCertificateResult = null;
        long timeSlept = 0L;
        while (exportCertificateResult == null && timeSlept < totalTimeout) {
            try {
                final ExportCertificateRequest exportCertificateRequest = new ExportCertificateRequest()
                        .withCertificateArn(acmArn)
                        .withPassphrase(ByteBuffer.wrap(passphrase.getBytes()));
                exportCertificateResult = awsCertificateManager.exportCertificate(exportCertificateRequest);

            } catch (final RequestInProgressException ex) {
                try {
                    Thread.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException iex) {
                    throw new RuntimeException(iex);
                }
            } catch (final ResourceNotFoundException | InvalidArnException ex) {
                LOG.error("Exception retrieving the certificate with arn: {}", acmArn, ex);
                throw ex;
            }
            timeSlept += SLEEP_INTERVAL;
        }
        if(exportCertificateResult != null) {
            final String decryptedPrivateKey = getDecryptedPrivateKey(exportCertificateResult.getPrivateKey(), passphrase);
            return new Certificate(exportCertificateResult.getCertificate(), decryptedPrivateKey);
        } else {
            throw new IllegalStateException(String.format("Exception retrieving certificate results. Time spent retrieving certificate is %d ms and total time out set is %d ms.", timeSlept, totalTimeout));
        }
    }

    private String getDecryptedPrivateKey(final String encryptedPrivateKey, final String keyPassword) {
        try {
            final PrivateKey rsaPrivateKey = encryptedPrivateKeyStringToPrivateKey(encryptedPrivateKey, keyPassword.toCharArray());
            return privateKeyAsString(rsaPrivateKey);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String privateKeyAsString(final PrivateKey key) throws IOException {
        final StringWriter sw = new StringWriter();
        try (JcaPEMWriter pw = new JcaPEMWriter(sw))
        {
            pw.writeObject(key);
        }
        return sw.toString();
    }

    private PrivateKey encryptedPrivateKeyStringToPrivateKey(final String encryptedPrivateKey, final char[] password)
            throws IOException, PKCSException {
        final PrivateKeyInfo pki;
        try (final PEMParser pemParser = new PEMParser(new StringReader(encryptedPrivateKey))) {
            final Object o = pemParser.readObject();
            if (o instanceof PKCS8EncryptedPrivateKeyInfo) { // encrypted private key in pkcs8-format
                LOG.debug("key in pkcs8 encoding");
                final PKCS8EncryptedPrivateKeyInfo epki = (PKCS8EncryptedPrivateKeyInfo) o;
                final JcePKCSPBEInputDecryptorProviderBuilder builder =
                        new JcePKCSPBEInputDecryptorProviderBuilder().setProvider(BOUNCY_CASTLE_PROVIDER);
                final InputDecryptorProvider idp = builder.build(password);
                pki = epki.decryptPrivateKeyInfo(idp);
            } else if (o instanceof PEMEncryptedKeyPair) { // encrypted private key in pkcs1-format
                LOG.debug("key in pkcs1 encoding");
                final PEMEncryptedKeyPair epki = (PEMEncryptedKeyPair) o;
                final PEMKeyPair pkp = epki.decryptKeyPair(new BcPEMDecryptorProvider(password));
                pki = pkp.getPrivateKeyInfo();
            } else if (o instanceof PEMKeyPair) { // unencrypted private key
                LOG.debug("key unencrypted");
                final PEMKeyPair pkp = (PEMKeyPair) o;
                pki = pkp.getPrivateKeyInfo();
            } else {
                throw new PKCSException("Invalid encrypted private key class: " + o != null ? o.getClass().getName() : null);
            }
            final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BOUNCY_CASTLE_PROVIDER);
            return converter.getPrivateKey(pki);
        }
    }
}
