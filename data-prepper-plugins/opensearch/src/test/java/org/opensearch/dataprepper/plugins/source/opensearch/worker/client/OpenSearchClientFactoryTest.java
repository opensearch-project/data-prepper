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
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.AuthConfig;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OpenSearchClientFactoryTest {

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private AuthConfig authConfig;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @BeforeEach
    void setup() {
        lenient().when(openSearchSourceConfiguration.getHosts()).thenReturn(List.of("http://localhost:9200"));
        when(openSearchSourceConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);
    }

    private OpenSearchClientFactory createObjectUnderTest() {
        return OpenSearchClientFactory.create(awsCredentialsSupplier);
    }

    @Test
    void provideOpenSearchClient_with_deprecated_username_and_password() {
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
    void provideOpenSearchClient_with_username_and_password() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getUsername()).thenReturn(username);
        when(authConfig.getPassword()).thenReturn(password);

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
    void provideElasticSearchClient_with_deprecated_username_and_password() {
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
    void provideElasticSearchClient_with_username_and_password() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getUsername()).thenReturn(username);
        when(authConfig.getPassword()).thenReturn(password);

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
    void provideOpenSearchClient_with_self_signed_certificate() {
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
    void provideElasticSearchClient_with_self_signed_certificate() {
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
