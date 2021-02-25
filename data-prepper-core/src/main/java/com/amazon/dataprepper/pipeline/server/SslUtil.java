package com.amazon.dataprepper.pipeline.server;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

public class SslUtil {

    public static SSLContext createSslContext(final String keyStoreFilePath,
                                              final String keyStorePassword,
                                              final String privateKeyPassword) {
        final SSLContext sslContext;

        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(Files.newInputStream(Path.of(keyStoreFilePath)), keyStorePassword.toCharArray());

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, privateKeyPassword.toCharArray());

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        } catch (Exception e) {
            throw new IllegalStateException("Problem loading keystore to create SSLContext", e);
        }

        return sslContext;
    }
}
