package com.amazon.dataprepper.pipeline.server;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class SslUtil {
    public static final String KEY_ALIAS = "key";
    private static final Pattern KEY_PATTERN = Pattern.compile(
            "-+BEGIN\\s+.*PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+" +
                    "([a-z0-9+/=\\r\\n]+)" +
                    "-+END\\s+.*PRIVATE\\s+KEY[^-]*-+",
            CASE_INSENSITIVE);

    private static final Pattern CERTIFICATE_PATTERN = Pattern.compile(
            "-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+" +
                    "([a-z0-9+/=\\r\\n]+)" +
                    "-+END\\s+.*CERTIFICATE[^-]*-+",
            CASE_INSENSITIVE);

    public KeyStore loadKeyStore(final File certificateChainFile,
                                 final File privateKeyFile)
            throws IOException, GeneralSecurityException {
        final PKCS8EncodedKeySpec encodedKeySpec = loadPrivateKey(privateKeyFile);

        PrivateKey key;
        try {
            final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            key = keyFactory.generatePrivate(encodedKeySpec);
        } catch (InvalidKeySpecException e) {
            final KeyFactory keyFactory = KeyFactory.getInstance("DSA");
            key = keyFactory.generatePrivate(encodedKeySpec);
        }

        final List<X509Certificate> certificateList = loadCertificateChain(certificateChainFile);

        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry(KEY_ALIAS,
                key,
                "".toCharArray(),
                certificateList.stream().toArray(Certificate[]::new));
        return keyStore;
    }

    private PKCS8EncodedKeySpec loadPrivateKey(final File keyFile)
            throws IOException, GeneralSecurityException {
        final String privateKeyString = new String(Files.readAllBytes(keyFile.toPath()), Charset.defaultCharset());

        final Matcher matcher = KEY_PATTERN.matcher(privateKeyString);
        if (!matcher.find()) {
            throw new KeyStoreException(String.format("%s contains no private key", keyFile));
        }
        byte[] encodedKey = Base64.getMimeDecoder().decode(matcher.group(1));

        return new PKCS8EncodedKeySpec(encodedKey);
    }

    private List<X509Certificate> loadCertificateChain(final File certificateChainFile)
            throws IOException, CertificateException {
        final String certificatesString = new String(Files.readAllBytes(certificateChainFile.toPath()), Charset.defaultCharset());

        final Matcher matcher = CERTIFICATE_PATTERN.matcher(certificatesString);
        final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        final List<X509Certificate> certificateList = new ArrayList<>();

        int start = 0;
        while (matcher.find(start)) {
            byte[] buffer = Base64.getMimeDecoder().decode(matcher.group(1));
            certificateList.add((X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(buffer)));
            start = matcher.end();
        }

        if (certificateList.isEmpty()) {
            throw new CertificateException(String.format("%s does not contain any certificates", certificateChainFile));
        }

        return certificateList;
    }
}