/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration.AWS_SERVERLESS;

@ExtendWith(MockitoExtension.class)
class ConnectionConfigurationTests {
    private static final String PROXY_PARAMETER = "proxy";
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final String TEST_PIPELINE_NAME = "Test-Pipeline";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("test-ca.pem")).getFile();
    private final String TEST_ROLE = "arn:aws:iam::123456789012:role/test-role";

    @Mock
    private ApacheHttpClient.Builder apacheHttpClientBuilder;
    @Mock
    private ApacheHttpClient apacheHttpClient;

    @Test
    void testReadConnectionConfigurationDefault() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    void testReadConnectionConfigurationAwsServerlessDefault() {
        final Map<String, Object> configMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, true, null, null, null, false);
        configMetadata.put(AWS_SERVERLESS, true);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertTrue(connectionConfiguration.isAwsServerless());
    }

    @Test
    void testReadConnectionConfigurationAwsOptionServerlessDefault() {
        final String testArn = TEST_ROLE;
        final Map<String, Object> configMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, true, false, null, testArn, TEST_CERT_PATH, false, Collections.emptyMap());
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertTrue(connectionConfiguration.isAwsServerless());
    }

    @Test
    void testCreateClientDefault() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientDefault() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        assertNotNull(openSearchClient);
        assertTrue(openSearchClient._transport() instanceof RestClientTransport);
        assertTrue(openSearchClient._transport().jsonpMapper() instanceof PreSerializedJsonpMapper);
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateOpenSearchClientAwsServerlessDefault() throws IOException {
        final Map<String, Object> configMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, true, null, null, null, false);
        configMetadata.put(AWS_SERVERLESS, true);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        when(apacheHttpClientBuilder.tlsTrustManagersProvider(any())).thenReturn(apacheHttpClientBuilder);
        when(apacheHttpClientBuilder.build()).thenReturn(apacheHttpClient);
        final OpenSearchClient openSearchClient;
        try (final MockedStatic<ApacheHttpClient> apacheHttpClientMockedStatic = mockStatic(ApacheHttpClient.class)) {
            apacheHttpClientMockedStatic.when(ApacheHttpClient::builder).thenReturn(apacheHttpClientBuilder);
            openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        }
        assertNotNull(openSearchClient);
        assertTrue(openSearchClient._transport() instanceof AwsSdk2Transport);
        assertTrue(openSearchClient._transport().jsonpMapper() instanceof PreSerializedJsonpMapper);
        verify(apacheHttpClientBuilder).tlsTrustManagersProvider(any());
        verify(apacheHttpClientBuilder).build();
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testReadConnectionConfigurationNoCert() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertEquals(TEST_USERNAME, connectionConfiguration.getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    void testCreateClientNoCert() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientNoCert() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        assertNotNull(openSearchClient);
        assertEquals(client.getLowLevelClient(), ((RestClientTransport) openSearchClient._transport()).restClient());
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateClientInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        assertNotNull(openSearchClient);
        assertEquals(client.getLowLevelClient(), ((RestClientTransport) openSearchClient._transport()).restClient());
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateClientWithCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientWithCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        assertNotNull(openSearchClient);
        assertEquals(client.getLowLevelClient(), ((RestClientTransport) openSearchClient._transport()).restClient());
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateClientWithAWSSigV4AndRegion() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, "us-west-2", null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-west-2", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testCreateClientWithAWSSigV4DefaultRegion() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    void testCreateClientWithAWSSigV4AndInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    void testCreateClientWithAWSSigV4AndCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    void testCreateClientWithAWSSigV4AndSTSRole() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.getAwsRegion(), equalTo("us-east-1"));
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(connectionConfiguration.getPipelineName(), equalTo(TEST_PIPELINE_NAME));

        final StsClient stsClient = mock(StsClient.class);
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
        when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.roleArn(anyString()))
                .thenReturn(assumeRoleRequestBuilder);

        final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
        when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
        when(stsClientBuilder.build()).thenReturn(stsClient);

        try(final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
            final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {

            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            connectionConfiguration.createClient();
        }

        verify(assumeRoleRequestBuilder).roleArn(TEST_ROLE);
        verify(assumeRoleRequestBuilder).roleSessionName(anyString());
        verify(assumeRoleRequestBuilder).build();
        verifyNoMoreInteractions(assumeRoleRequestBuilder);
    }

    @Test
    void testCreateOpenSearchClientWithAWSSigV4AndSTSRole() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.getAwsRegion(), equalTo("us-east-1"));
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(connectionConfiguration.getPipelineName(), equalTo(TEST_PIPELINE_NAME));

        final StsClient stsClient = mock(StsClient.class);
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
        when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.roleArn(anyString()))
                .thenReturn(assumeRoleRequestBuilder);

        final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
        when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
        when(stsClientBuilder.build()).thenReturn(stsClient);

        final OpenSearchClient openSearchClient;
        try(final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
            final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {

            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            final RestHighLevelClient client = connectionConfiguration.createClient();
            openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        }
        assertNotNull(openSearchClient);
        assertTrue(openSearchClient._transport() instanceof AwsSdk2Transport);
        final AwsSdk2Transport opensearchTransport = (AwsSdk2Transport) openSearchClient._transport();
        assertTrue(opensearchTransport.options().credentials() instanceof StsAssumeRoleCredentialsProvider);
        verify(assumeRoleRequestBuilder, times(2)).roleArn(TEST_ROLE);
        verify(assumeRoleRequestBuilder, times(2)).roleSessionName(anyString());
        verify(assumeRoleRequestBuilder, times(2)).build();
        verifyNoMoreInteractions(assumeRoleRequestBuilder);
    }

    @Test
    void testCreateClientWithAWSOption() {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final String testArn = TEST_ROLE;
        final Map<String, Object> configurationMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, false, true, null, testArn, TEST_CERT_PATH, false, Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final StsClient stsClient = mock(StsClient.class);
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
        when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.roleArn(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.overrideConfiguration(any(Consumer.class)))
                .thenReturn(assumeRoleRequestBuilder);

        final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
        when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
        when(stsClientBuilder.build()).thenReturn(stsClient);

        try(final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
            final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {

            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            connectionConfiguration.createClient();
        }

        final ArgumentCaptor<Consumer<AwsRequestOverrideConfiguration.Builder>> configurationCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(assumeRoleRequestBuilder).overrideConfiguration(configurationCaptor.capture());
        verify(assumeRoleRequestBuilder).roleArn(testArn);
        verify(assumeRoleRequestBuilder).roleSessionName(anyString());
        verify(assumeRoleRequestBuilder).build();
        verifyNoMoreInteractions(assumeRoleRequestBuilder);

        final Consumer<AwsRequestOverrideConfiguration.Builder> actualOverride = configurationCaptor.getValue();

        final AwsRequestOverrideConfiguration.Builder configurationBuilder = mock(AwsRequestOverrideConfiguration.Builder.class);
        actualOverride.accept(configurationBuilder);
        verify(configurationBuilder).putHeader(headerName1, headerValue1);
        verify(configurationBuilder).putHeader(headerName2, headerValue2);
        verifyNoMoreInteractions(configurationBuilder);
    }

    @Test
    void testCreateOpenSearchClientWithAWSOption() {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final String testArn = TEST_ROLE;
        final Map<String, Object> configurationMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, false, true, null, testArn, TEST_CERT_PATH, false, Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final StsClient stsClient = mock(StsClient.class);
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
        when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.roleArn(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.overrideConfiguration(any(Consumer.class)))
                .thenReturn(assumeRoleRequestBuilder);

        final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
        when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
        when(stsClientBuilder.build()).thenReturn(stsClient);

        final OpenSearchClient openSearchClient;
        try(final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
            final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {

            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            final RestHighLevelClient client = connectionConfiguration.createClient();
            openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        }

        final ArgumentCaptor<Consumer<AwsRequestOverrideConfiguration.Builder>> configurationCaptor = ArgumentCaptor.forClass(Consumer.class);

        assertNotNull(openSearchClient);
        assertTrue(openSearchClient._transport() instanceof AwsSdk2Transport);
        final AwsSdk2Transport opensearchTransport = (AwsSdk2Transport) openSearchClient._transport();
        assertTrue(opensearchTransport.options().credentials() instanceof StsAssumeRoleCredentialsProvider);
        verify(assumeRoleRequestBuilder, times(2)).overrideConfiguration(configurationCaptor.capture());
        verify(assumeRoleRequestBuilder, times(2)).roleArn(testArn);
        verify(assumeRoleRequestBuilder, times(2)).roleSessionName(anyString());
        verify(assumeRoleRequestBuilder, times(2)).build();
        verifyNoMoreInteractions(assumeRoleRequestBuilder);

        final Consumer<AwsRequestOverrideConfiguration.Builder> actualOverride = configurationCaptor.getValue();

        final AwsRequestOverrideConfiguration.Builder configurationBuilder = mock(AwsRequestOverrideConfiguration.Builder.class);
        actualOverride.accept(configurationBuilder);
        verify(configurationBuilder).putHeader(headerName1, headerValue1);
        verify(configurationBuilder).putHeader(headerName2, headerValue2);
        verifyNoMoreInteractions(configurationBuilder);
    }

    @Test
    void testCreateClientWithAWSSigV4AndHeaderOverrides() {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        configurationMetadata.put("aws_sts_header_overrides", Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final StsClient stsClient = mock(StsClient.class);
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
        when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.roleArn(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.overrideConfiguration(any(Consumer.class)))
                .thenReturn(assumeRoleRequestBuilder);

        final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
        when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
        when(stsClientBuilder.build()).thenReturn(stsClient);

        try(final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
            final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {

            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            connectionConfiguration.createClient();
        }

        final ArgumentCaptor<Consumer<AwsRequestOverrideConfiguration.Builder>> configurationCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(assumeRoleRequestBuilder).overrideConfiguration(configurationCaptor.capture());
        verify(assumeRoleRequestBuilder).roleArn(TEST_ROLE);
        verify(assumeRoleRequestBuilder).roleSessionName(anyString());
        verify(assumeRoleRequestBuilder).build();
        verifyNoMoreInteractions(assumeRoleRequestBuilder);

        final Consumer<AwsRequestOverrideConfiguration.Builder> actualOverride = configurationCaptor.getValue();

        final AwsRequestOverrideConfiguration.Builder configurationBuilder = mock(AwsRequestOverrideConfiguration.Builder.class);
        actualOverride.accept(configurationBuilder);
        verify(configurationBuilder).putHeader(headerName1, headerValue1);
        verify(configurationBuilder).putHeader(headerName2, headerValue2);
        verifyNoMoreInteractions(configurationBuilder);
    }

    @Test
    void testCreateOpenSearchClientWithAWSSigV4AndHeaderOverrides() {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        configurationMetadata.put("aws_sts_header_overrides", Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final StsClient stsClient = mock(StsClient.class);
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
        when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.roleArn(anyString()))
                .thenReturn(assumeRoleRequestBuilder);
        when(assumeRoleRequestBuilder.overrideConfiguration(any(Consumer.class)))
                .thenReturn(assumeRoleRequestBuilder);

        final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
        when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
        when(stsClientBuilder.build()).thenReturn(stsClient);

        final OpenSearchClient openSearchClient;
        try(final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
            final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {

            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            final RestHighLevelClient client = connectionConfiguration.createClient();
            openSearchClient = connectionConfiguration.createOpenSearchClient(client);
        }

        assertNotNull(openSearchClient);
        assertTrue(openSearchClient._transport() instanceof AwsSdk2Transport);
        final AwsSdk2Transport opensearchTransport = (AwsSdk2Transport) openSearchClient._transport();
        assertTrue(opensearchTransport.options().credentials() instanceof StsAssumeRoleCredentialsProvider);

        final ArgumentCaptor<Consumer<AwsRequestOverrideConfiguration.Builder>> configurationCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(assumeRoleRequestBuilder, times(2)).overrideConfiguration(configurationCaptor.capture());
        verify(assumeRoleRequestBuilder, times(2)).roleArn(TEST_ROLE);
        verify(assumeRoleRequestBuilder, times(2)).roleSessionName(anyString());
        verify(assumeRoleRequestBuilder, times(2)).build();
        verifyNoMoreInteractions(assumeRoleRequestBuilder);

        final Consumer<AwsRequestOverrideConfiguration.Builder> actualOverride = configurationCaptor.getValue();

        final AwsRequestOverrideConfiguration.Builder configurationBuilder = mock(AwsRequestOverrideConfiguration.Builder.class);
        actualOverride.accept(configurationBuilder);
        verify(configurationBuilder).putHeader(headerName1, headerValue1);
        verify(configurationBuilder).putHeader(headerName2, headerValue2);
        verifyNoMoreInteractions(configurationBuilder);
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_HostIP() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "121.121.121.121:80";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client));
        client.close();
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_HostName() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:80";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client));
        client.close();
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_SchemeProvided() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "http://example.com:4350";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client));
        client.close();
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_InvalidPort() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:port";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_NoPort() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_PortNotInRange() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:888888";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_NotHttp() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "socket://example.com:port";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    void testCreateClient_WithConnectionConfigurationBuilder_ProxyOptionalObjectShouldNotBeNull() throws IOException {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(TEST_HOSTS);
        final ConnectionConfiguration connectionConfiguration = builder.build();
        assertEquals(Optional.empty(), connectionConfiguration.getProxy());
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    private PluginSetting generatePluginSetting(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean awsSigv4, final String awsRegion,
            final String awsStsRoleArn, final String certPath, final boolean insecure) {
        final Map<String, Object> metadata = generateConfigurationMetadata(hosts, username, password, connectTimeout, socketTimeout, awsSigv4, awsRegion, awsStsRoleArn, certPath, insecure);
        return getPluginSettingByConfigurationMetadata(metadata);
    }

    private Map<String, Object> generateConfigurationMetadata(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean awsSigv4, final String awsRegion,
            final String awsStsRoleArn, final String certPath, final boolean insecure) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("hosts", hosts);
        metadata.put("username", username);
        metadata.put("password", password);
        metadata.put("connect_timeout", connectTimeout);
        metadata.put("socket_timeout", socketTimeout);
        metadata.put("aws_sigv4", awsSigv4);
        if (awsRegion != null) {
            metadata.put("aws_region", awsRegion);
        }
        metadata.put("aws_sts_role_arn", awsStsRoleArn);
        metadata.put("cert", certPath);
        metadata.put("insecure", insecure);
        return metadata;
    }


    private Map<String, Object> generateConfigurationMetadataWithAwsOption(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean serverless, final boolean awsSigv4, final String awsRegion,
            final String awsStsRoleArn, final String certPath, final boolean insecure, Map<String, String> headerOverridesMap) {
        final Map<String, Object> metadata = new HashMap<>();
        final Map<String, Object> awsOptionMetadata = new HashMap<>();
        metadata.put("hosts", hosts);
        metadata.put("username", username);
        metadata.put("password", password);
        metadata.put("connect_timeout", connectTimeout);
        metadata.put("socket_timeout", socketTimeout);
        if (awsRegion != null) {
            awsOptionMetadata.put("region", awsRegion);
        }
        awsOptionMetadata.put("serverless", serverless);
        awsOptionMetadata.put("sts_role_arn", awsStsRoleArn);
        awsOptionMetadata.put("sts_header_overrides", headerOverridesMap);
        metadata.put("aws", awsOptionMetadata);
        metadata.put("cert", certPath);
        metadata.put("insecure", insecure);
        return metadata;
    }

    private PluginSetting getPluginSettingByConfigurationMetadata(final Map<String, Object> metadata) {
        final PluginSetting pluginSetting = new PluginSetting("opensearch", metadata);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return pluginSetting;
    }
}
