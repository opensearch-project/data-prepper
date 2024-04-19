/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OpenSearchClientFactoryTest {
    private static final String TEST_CERTIFICATE = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDYTCCAkmgAwIBAgIUFBUALAhfpezYbYw6AtH96tizPTIwDQYJKoZIhvcNAQEL\n" +
            "BQAwWTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM\n" +
            "GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDESMBAGA1UEAwwJMTI3LjAuMC4xMB4X\n" +
            "DTI0MDQxMTE3MzcyMloXDTM0MDQwOTE3MzcyMlowWTELMAkGA1UEBhMCQVUxEzAR\n" +
            "BgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5\n" +
            "IEx0ZDESMBAGA1UEAwwJMTI3LjAuMC4xMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n" +
            "MIIBCgKCAQEAuaS6lrpg38XT5wmukekr8NSXcO70yhMRLF29mAXasYeumtHVDR/p\n" +
            "f8vTE4l+b36kRuaew4htGRZQcWJBdPoCDkDHA3+5z5t9Fe3nR9FzIA+E/KjyMCEq\n" +
            "xNgc9OIN9UyBbbneMkR24W8LkAywxk3euXgj46+7SGFHAdNLqC72Yl3W1E32rQAR\n" +
            "c6zQIJ45uogqU19QJHCBBfJA+IFylwtNGWfNbvdvGCXx5FZnM3q4rCxNr9F+LBsS\n" +
            "xWFlXGMHXo2+bMBGIBXPGbGXpad/jVgxjM6zV5vnG1g8GDxaHaM+3LjJwa7eQYVA\n" +
            "ogetug9wqesxkf+Nic/rpB6J7zM2iwY0AQIDAQABoyEwHzAdBgNVHQ4EFgQUept4\n" +
            "OD2pNRYswtfrOqnOgx4QtjYwDQYJKoZIhvcNAQELBQADggEBACU+Qjmf35BOYjDj\n" +
            "TX1f6bhgwsHP/WHwWWKIhVSOB0CFHoizzQyLREgWWLkKs3Ye3q9DXku0saMfIerq\n" +
            "S7hqDA8hNVJVyllh2FuuNQVkmOKJFTwev2n3OvSyz4mxWW3UNJb/YTuK93nNHVVo\n" +
            "/3+lQg0sJRhSMs/GmR/Hn7/py2/2pucFJrML/Dtjv7SwrOXptWn+GCB+3bUpfNdg\n" +
            "sHeZEv9vpbQDzp1Lux7l3pMzwsi6HU4xTyHClBD7V8S2MUExMXDF+Cr4g7lmOb02\n" +
            "Bw0dTI7afBMI8n5YgTX78YMGqbO/WJ3bOc26P2i7RrRIhOXw69UZff2JwYAnX6Op\n" +
            "zHOodz4=\n" +
            "-----END CERTIFICATE-----";

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @Captor
    private ArgumentCaptor<Path> certificatePathCaptor;

    @BeforeEach
    void setup() {
        lenient().when(openSearchSourceConfiguration.getHosts()).thenReturn(List.of("http://localhost:9200"));
        when(openSearchSourceConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);
    }

    private OpenSearchClientFactory createObjectUnderTest() {
        return OpenSearchClientFactory.create(awsCredentialsSupplier);
    }

    @Test
    void provideOpenSearchClient_with_username_and_password() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);

        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);
        when(connectionConfiguration.isInsecure()).thenReturn(true);

        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(null);

        final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
        assertThat(openSearchClient, notNullValue());

        verifyNoInteractions(awsCredentialsSupplier);

    }

    @Test
    void provideElasticSearchClient_with_username_and_password() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);

        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);
        when(connectionConfiguration.isInsecure()).thenReturn(true);

        final ElasticsearchClient elasticsearchClient = createObjectUnderTest().provideElasticSearchClient(openSearchSourceConfiguration);
        assertThat(elasticsearchClient, notNullValue());

        verifyNoInteractions(awsCredentialsSupplier);
    }

    @Test
    void provideElasticSearchClient_with_aws_auth() {
        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final String stsRoleArn = "arn:aws:iam::123456789012:role/my-role";
        when(awsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture())).thenReturn(awsCredentialsProvider);

        final ElasticsearchClient elasticsearchClient = createObjectUnderTest().provideElasticSearchClient(openSearchSourceConfiguration);
        assertThat(elasticsearchClient, notNullValue());

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void provideOpenSearchClient_with_aws_auth() {
        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final String stsRoleArn = "arn:aws:iam::123456789012:role/my-role";
        when(awsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(awsAuthenticationConfiguration.isServerlessCollection()).thenReturn(false);
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture())).thenReturn(awsCredentialsProvider);

        final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
        assertThat(openSearchClient, notNullValue());

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void provideElasticSearchClient_with_auth_disabled() {
        when(openSearchSourceConfiguration.isAuthenticationDisabled()).thenReturn(true);

        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);
        when(connectionConfiguration.isInsecure()).thenReturn(true);

        final ElasticsearchClient elasticsearchClient = createObjectUnderTest().provideElasticSearchClient(openSearchSourceConfiguration);
        assertThat(elasticsearchClient, notNullValue());

        verifyNoInteractions(awsCredentialsSupplier);
        verify(openSearchSourceConfiguration, never()).getUsername();
        verify(openSearchSourceConfiguration, never()).getPassword();
    }

    @Test
    void provideOpenSearchClient_with_auth_disabled() {
        when(openSearchSourceConfiguration.isAuthenticationDisabled()).thenReturn(true);

        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);
        when(connectionConfiguration.isInsecure()).thenReturn(true);

        final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
        assertThat(openSearchClient, notNullValue());

        verifyNoInteractions(awsCredentialsSupplier);
        verify(openSearchSourceConfiguration, never()).getUsername();
        verify(openSearchSourceConfiguration, never()).getPassword();
    }

    @Test
    void provideOpenSearchClient_with_aws_auth_and_serverless_flag_true() {
        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final String stsRoleArn = "arn:aws:iam::123456789012:role/my-role";
        when(awsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(awsAuthenticationConfiguration.isServerlessCollection()).thenReturn(true);
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture())).thenReturn(awsCredentialsProvider);

        final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
        assertThat(openSearchClient, notNullValue());

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void provideOpenSearchClient_with_deprecated_self_signed_certificate() {
        final Path path = mock(Path.class);
        final SSLContext sslContext = mock(SSLContext.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        when(connectionConfiguration.getCertPath()).thenReturn(path);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(path))
                    .thenReturn(sslContext);
            final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createSSLContext(path));
            assertThat(openSearchClient, notNullValue());
        }
    }

    @Test
    void provideOpenSearchClient_with_self_signed_certificate_filepath() {
        final String certificate = UUID.randomUUID().toString();
        final SSLContext sslContext = mock(SSLContext.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        when(connectionConfiguration.getCertificate()).thenReturn(certificate);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(any(Path.class)))
                    .thenReturn(sslContext);
            final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createSSLContext(certificatePathCaptor.capture()));
            assertThat(openSearchClient, notNullValue());
            assertThat(certificatePathCaptor.getValue(), equalTo(certificate));
        }
    }

    @Test
    void provideOpenSearchClient_with_self_signed_certificate_content() {
        final SSLContext sslContext = mock(SSLContext.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        when(connectionConfiguration.getCertificate()).thenReturn(TEST_CERTIFICATE);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(TEST_CERTIFICATE))
                    .thenReturn(sslContext);
            final OpenSearchClient openSearchClient = createObjectUnderTest().provideOpenSearchClient(openSearchSourceConfiguration);
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createSSLContext(TEST_CERTIFICATE));
            assertThat(openSearchClient, notNullValue());
        }
    }

    @Test
    void provideElasticSearchClient_with_deprecated_self_signed_certificate_filepath() {
        final Path path = mock(Path.class);
        final SSLContext sslContext = mock(SSLContext.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        when(connectionConfiguration.getCertPath()).thenReturn(path);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(path))
                    .thenReturn(sslContext);
            final ElasticsearchClient elasticsearchClient = createObjectUnderTest().provideElasticSearchClient(openSearchSourceConfiguration);
            assertThat(elasticsearchClient, notNullValue());
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createSSLContext(path));
        }
    }

    @Test
    void provideElasticSearchClient_with_self_signed_certificate_filepath() {
        final String certificate = UUID.randomUUID().toString();
        final SSLContext sslContext = mock(SSLContext.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        when(connectionConfiguration.getCertificate()).thenReturn(certificate);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(any(Path.class)))
                    .thenReturn(sslContext);
            final ElasticsearchClient elasticsearchClient = createObjectUnderTest().provideElasticSearchClient(openSearchSourceConfiguration);
            assertThat(elasticsearchClient, notNullValue());
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createSSLContext(certificatePathCaptor.capture()));
            assertThat(certificatePathCaptor.getValue().toString(), equalTo(certificate));
        }
    }

    @Test
    void provideElasticSearchClient_with_self_signed_certificate_content() {
        final SSLContext sslContext = mock(SSLContext.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        when(connectionConfiguration.getCertificate()).thenReturn(TEST_CERTIFICATE);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            trustStoreProviderMockedStatic.when(() -> TrustStoreProvider.createSSLContext(TEST_CERTIFICATE))
                    .thenReturn(sslContext);
            final ElasticsearchClient elasticsearchClient = createObjectUnderTest().provideElasticSearchClient(openSearchSourceConfiguration);
            assertThat(elasticsearchClient, notNullValue());
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createSSLContext(TEST_CERTIFICATE));
        }
    }

    @Test
    void createSdkAsyncHttpClient_with_self_signed_certificate() {
        final Path path = mock(Path.class);
        final Duration duration = mock(Duration.class);
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        lenient().when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        lenient().when(openSearchSourceConfiguration.getPassword()).thenReturn(password);
        lenient().when(connectionConfiguration.getConnectTimeout()).thenReturn(duration);
        lenient().when(openSearchSourceConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);
        lenient().when(connectionConfiguration.getCertPath()).thenReturn(path);
        try (MockedStatic<TrustStoreProvider> trustStoreProviderMockedStatic = mockStatic(TrustStoreProvider.class)) {
            final SdkAsyncHttpClient sdkAsyncHttpClient = createObjectUnderTest().createSdkAsyncHttpClient(openSearchSourceConfiguration);
            assertThat(sdkAsyncHttpClient, notNullValue());
            trustStoreProviderMockedStatic.verify(() -> TrustStoreProvider.createTrustManager(path));
        }
    }
}
