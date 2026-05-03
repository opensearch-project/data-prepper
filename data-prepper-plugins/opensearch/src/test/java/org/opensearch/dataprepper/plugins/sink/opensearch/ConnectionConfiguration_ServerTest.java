/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.MainResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.jsonResponse;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectionConfiguration_ServerTest {
    private static WireMockServer wireMockServer;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private String host;

    private String clusterUuid;

    @BeforeAll
    static void setUpAll() {
        wireMockServer = new WireMockServer(options()
                .httpDisabled(true)
                .dynamicHttpsPort()
                .keystorePath("src/test/resources/test_keystore.jks")
                .keystorePassword("password")
                .keyManagerPassword("password")
        );

        wireMockServer.start();
    }

    @AfterAll
    static void tearDownAll() {
        wireMockServer.stop();
    }

    @BeforeEach
    void setUp() {
        host = "https://localhost:" + wireMockServer.httpsPort();

        clusterUuid = UUID.randomUUID().toString();
        final Map<String, Object> responseBody = Map.of(
                "name", "opensearch",
                "cluster_name", "opensearch",
                "cluster_uuid", clusterUuid,
                "version", Map.of(
                        "number", "2.10.0",
                        "build_hash", "abcdefg",
                        "build_date", "20241212",
                        "build_type", "testing",
                        "distribution", "datapreppertesting",
                        "build_snapshot", "false",
                        "lucene_version", "8",
                        "minimum_wire_compatibility_version", "2.10.0",
                        "minimum_index_compatibility_version", "2.10.0"
                ),
                "tagline", "You Know, for Search"
        );
        wireMockServer.stubFor(get("/").willReturn(jsonResponse(responseBody, 200)));
    }

    @Nested
    class DefaultConfiguration {
        private ConnectionConfiguration createObjectUnderTest() {
            return new ConnectionConfiguration.Builder(Collections.singletonList(host))
                    .build();
        }

        @Test
        void createClient_will_not_trust_self_signed_certificates_by_default() {
            final RestHighLevelClient client = createObjectUnderTest().createClient(awsCredentialsSupplier);
            assertThat(client, notNullValue());

            assertThrows(SSLHandshakeException.class, () -> client.info(RequestOptions.DEFAULT));
        }

        @Test
        void createOpenSearchClient_will_not_trust_self_signed_certificates_by_default() {
            final ConnectionConfiguration objectUnderTest = createObjectUnderTest();
            final OpenSearchClient openSearchClient = objectUnderTest.createOpenSearchClient(objectUnderTest.createClient(awsCredentialsSupplier), awsCredentialsSupplier);
            assertThat(openSearchClient, notNullValue());

            assertThrows(SSLHandshakeException.class, openSearchClient::info);
        }
    }

    @Nested
    class DefaultSigV4Configuration {
        @BeforeEach
        void setUp() {
            when(awsCredentialsSupplier.getProvider(any())).thenReturn(AnonymousCredentialsProvider.create());
        }

        private ConnectionConfiguration createObjectUnderTest() {
            return new ConnectionConfiguration.Builder(Collections.singletonList(host))
                    .withAwsSigv4(true)
                    .withAwsRegion("us-east-1")
                    .build();
        }

        @Test
        void createClient_will_not_trust_self_signed_certificates_by_default() {
            final RestHighLevelClient client = createObjectUnderTest().createClient(awsCredentialsSupplier);
            assertThat(client, notNullValue());

            assertThrows(SSLHandshakeException.class, () -> client.info(RequestOptions.DEFAULT));
        }

        @Test
        void createOpenSearchClient_will_not_trust_self_signed_certificates_by_default() {
            final ConnectionConfiguration objectUnderTest = createObjectUnderTest();
            final OpenSearchClient openSearchClient = objectUnderTest.createOpenSearchClient(objectUnderTest.createClient(awsCredentialsSupplier), awsCredentialsSupplier);
            assertThat(openSearchClient, notNullValue());

            assertThrows(SSLHandshakeException.class, openSearchClient::info);
        }
    }

    @Nested
    class InsecureConfiguration {
        private ConnectionConfiguration createObjectUnderTest() {
            return new ConnectionConfiguration.Builder(Collections.singletonList(host))
                    .withInsecure(true)
                    .build();
        }

        @Test
        void createClient_will_trust_self_signed_certificates_if_insecure() throws IOException {
            final RestHighLevelClient client = createObjectUnderTest().createClient(awsCredentialsSupplier);
            assertThat(client, notNullValue());

            final MainResponse infoResponse = client.info(RequestOptions.DEFAULT);

            assertThat(infoResponse, notNullValue());
            assertThat(infoResponse.getClusterName(), equalTo("opensearch"));
            assertThat(infoResponse.getClusterUuid(), equalTo(clusterUuid));
        }


        @Test
        void createOpenSearchClient_will_trust_self_signed_certificates_if_insecure() throws IOException {
            final ConnectionConfiguration objectUnderTest = createObjectUnderTest();
            final OpenSearchClient openSearchClient = objectUnderTest.createOpenSearchClient(objectUnderTest.createClient(awsCredentialsSupplier), awsCredentialsSupplier);
            assertThat(openSearchClient, notNullValue());

            final InfoResponse infoResponse = openSearchClient.info();

            assertThat(infoResponse, notNullValue());
            assertThat(infoResponse.clusterName(), equalTo("opensearch"));
            assertThat(infoResponse.clusterUuid(), equalTo(clusterUuid));
        }
    }

    @Nested
    class ClientCertificateConfiguration {
        private WireMockServer mtlsWireMockServer;
        private String mtlsHost;

        private final String clientCertPath = Objects.requireNonNull(
                getClass().getClassLoader().getResource("test-client-cert.pem")).getFile();
        private final String clientKeyPath = Objects.requireNonNull(
                getClass().getClassLoader().getResource("test-client-key.pem")).getFile();

        @BeforeEach
        void setUp() throws Exception {
            final String trustStorePath = createTrustStoreFromPem();

            mtlsWireMockServer = new WireMockServer(options()
                    .httpDisabled(true)
                    .dynamicHttpsPort()
                    .keystorePath("src/test/resources/test_keystore.jks")
                    .keystorePassword("password")
                    .keyManagerPassword("password")
                    .needClientAuth(true)
                    .trustStorePath(trustStorePath)
                    .trustStorePassword("changeit")
            );
            mtlsWireMockServer.start();

            mtlsHost = "https://localhost:" + mtlsWireMockServer.httpsPort();

            final Map<String, Object> responseBody = Map.of(
                    "name", "opensearch",
                    "cluster_name", "opensearch",
                    "cluster_uuid", UUID.randomUUID().toString(),
                    "version", Map.of(
                            "number", "2.10.0",
                            "build_hash", "abcdefg",
                            "build_date", "20241212",
                            "build_type", "testing",
                            "distribution", "datapreppertesting",
                            "build_snapshot", "false",
                            "lucene_version", "8",
                            "minimum_wire_compatibility_version", "2.10.0",
                            "minimum_index_compatibility_version", "2.10.0"
                    ),
                    "tagline", "You Know, for Search"
            );
            mtlsWireMockServer.stubFor(get("/").willReturn(jsonResponse(responseBody, 200)));
        }

        private String createTrustStoreFromPem() throws Exception {
            final CertificateFactory factory = CertificateFactory.getInstance("X.509");
            final Certificate caCert;
            try (InputStream is = getClass().getClassLoader().getResourceAsStream("test-client-ca.pem")) {
                caCert = factory.generateCertificate(is);
            }
            final KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(null, "changeit".toCharArray());
            trustStore.setCertificateEntry("client-ca", caCert);
            final Path trustStoreFile = Files.createTempFile("test-client-truststore-", ".jks");
            trustStoreFile.toFile().deleteOnExit();
            try (var out = Files.newOutputStream(trustStoreFile)) {
                trustStore.store(out, "changeit".toCharArray());
            }
            return trustStoreFile.toString();
        }

        @AfterEach
        void tearDown() {
            if (mtlsWireMockServer != null) {
                mtlsWireMockServer.stop();
            }
        }

        @Test
        void createClient_with_client_cert_succeeds_on_mtls_server() throws IOException {
            final ConnectionConfiguration objectUnderTest = new ConnectionConfiguration.Builder(Collections.singletonList(mtlsHost))
                    .withInsecure(true)
                    .withClientCert(clientCertPath)
                    .withClientKey(clientKeyPath)
                    .build();

            final RestHighLevelClient client = objectUnderTest.createClient(awsCredentialsSupplier);
            assertThat(client, notNullValue());

            final MainResponse infoResponse = client.info(RequestOptions.DEFAULT);
            assertThat(infoResponse, notNullValue());
            assertThat(infoResponse.getClusterName(), equalTo("opensearch"));
        }

        @Test
        void createClient_without_client_cert_fails_on_mtls_server() {
            final ConnectionConfiguration objectUnderTest = new ConnectionConfiguration.Builder(Collections.singletonList(mtlsHost))
                    .withInsecure(true)
                    .build();

            final RestHighLevelClient client = objectUnderTest.createClient(awsCredentialsSupplier);
            assertThat(client, notNullValue());

            final Exception exception = assertThrows(Exception.class, () -> client.info(RequestOptions.DEFAULT));
            assertThat(exception, anyOf(
                    instanceOf(SSLHandshakeException.class),
                    instanceOf(javax.net.ssl.SSLException.class),
                    instanceOf(IOException.class)));
        }
    }
}
