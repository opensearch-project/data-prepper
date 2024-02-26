package org.opensearch.dataprepper.plugins.truststore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class TrustStoreProviderTest {

    private TrustStoreProvider trustStoreProvider;

    @BeforeEach
    void setUp() {
        trustStoreProvider = new TrustStoreProvider();
    }

    @Test
    void createTrustManagerWithCertificatePath() {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final TrustManager[] trustManagers = trustStoreProvider.createTrustManager(certFilePath);
        assertThat(trustManagers, is(notNullValue()));
    }

    @Test
    void createTrustManagerWithInvalidCertificatePath() {
        final Path certFilePath = Path.of("data/certificate/cert_doesnt_exist.crt");
        assertThrows(RuntimeException.class, () -> trustStoreProvider.createTrustManager(certFilePath));
    }

    @Test
    void createTrustManagerWithCertificateContent() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final String certificateContent = Files.readString(certFilePath);
        final TrustManager[] trustManagers = trustStoreProvider.createTrustManager(certificateContent);
        assertThat(trustManagers, is(notNullValue()));
    }

    @Test
    void createTrustManagerWithInvalidCertificateContent() {
        assertThrows(RuntimeException.class, () -> trustStoreProvider.createTrustManager("invalid certificate content"));
    }

    @Test
    void createTrustAllManager() {
        final TrustManager[] trustManagers = trustStoreProvider.createTrustAllManager();
        assertThat(trustManagers, is(notNullValue()));
        assertThat(trustManagers, is(arrayWithSize(1)));
        assertThat(trustManagers[0], is(instanceOf(X509TrustAllManager.class)));
    }

    @Test
    void createSSLContextWithCertificatePath() {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final SSLContext sslContext = trustStoreProvider.createSSLContext(certFilePath);
        assertThat(sslContext, is(notNullValue()));
    }

    @Test
    void createSSLContextWithCertificateContent() throws IOException {
        final Path certFilePath = Path.of("data/certificate/test_cert.crt");
        final String certificateContent = Files.readString(certFilePath);
        final SSLContext sslContext = trustStoreProvider.createSSLContext(certificateContent);
        assertThat(sslContext, is(notNullValue()));
    }

    @Test
    void createSSLContextWithTrustAllStrategy() {
        final SSLContext sslContext = trustStoreProvider.createSSLContextWithTrustAllStrategy();
        assertThat(sslContext, is(notNullValue()));
    }
}