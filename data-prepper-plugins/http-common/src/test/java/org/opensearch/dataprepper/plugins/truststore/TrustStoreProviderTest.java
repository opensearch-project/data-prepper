package org.opensearch.dataprepper.plugins.truststore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class TrustStoreProviderTest {

    @Test
    void createTrustManagerWithCertificatePath() {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final TrustManager[] trustManagers = TrustStoreProvider.createTrustManager(certFilePath);
        assertThat(trustManagers, is(notNullValue()));
    }

    @Test
    void createTrustManagerWithInvalidCertificatePath() {
        final Path certFilePath = Path.of("data/certificate/cert_doesnt_exist.crt");
        assertThrows(RuntimeException.class, () -> TrustStoreProvider.createTrustManager(certFilePath));
    }

    @Test
    void createTrustManagerWithCertificateContent() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final String certificateContent = Files.readString(certFilePath);
        final TrustManager[] trustManagers = TrustStoreProvider.createTrustManager(certificateContent);
        assertThat(trustManagers, is(notNullValue()));
    }

    @Test
    void createTrustManagerWithInvalidCertificateContent() {
        assertThrows(RuntimeException.class, () -> TrustStoreProvider.createTrustManager("invalid certificate content"));
    }

    @Test
    void createTrustAllManager() {
        final TrustManager[] trustManagers = TrustStoreProvider.createTrustAllManager();
        assertThat(trustManagers, is(notNullValue()));
        assertThat(trustManagers, is(arrayWithSize(1)));
        assertThat(trustManagers[0], is(instanceOf(X509TrustAllManager.class)));
    }

    @Test
    void createSSLContextWithCertificatePath() {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final SSLContext sslContext = TrustStoreProvider.createSSLContext(certFilePath);
        assertThat(sslContext, is(notNullValue()));
    }

    @Test
    void createSSLContextWithTrustStorePathAndPassword() throws Exception {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final KeyStore trustStore = mock(KeyStore.class);
        final TrustManagerFactory trustManagerFactory = mock(TrustManagerFactory.class);
        try (MockedStatic<KeyStore> keyStoreMockedStatic = mockStatic(KeyStore.class);
             MockedStatic<TrustManagerFactory> trustManagerFactoryMockedStatic = mockStatic(TrustManagerFactory.class)) {
            keyStoreMockedStatic.when(() -> KeyStore.getInstance("JKS")).thenReturn(trustStore);
            trustManagerFactoryMockedStatic.when(() ->
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())).thenReturn(trustManagerFactory);
            final SSLContext sslContext = TrustStoreProvider.createSSLContext(certFilePath, UUID.randomUUID().toString());
            assertThat(sslContext, is(notNullValue()));
        }
    }

    @Test
    void createSSLContextWithCertificateContent() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final String certificateContent = Files.readString(certFilePath);
        final SSLContext sslContext = TrustStoreProvider.createSSLContext(certificateContent);
        assertThat(sslContext, is(notNullValue()));
    }

    @Test
    void createSSLContextWithTrustAllStrategy() {
        final SSLContext sslContext = TrustStoreProvider.createSSLContextWithTrustAllStrategy();
        assertThat(sslContext, is(notNullValue()));
    }
}