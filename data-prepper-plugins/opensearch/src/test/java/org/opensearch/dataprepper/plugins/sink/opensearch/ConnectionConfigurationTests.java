/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration.SERVERLESS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DISTRIBUTION_VERSION;

@ExtendWith(MockitoExtension.class)
class ConnectionConfigurationTests {
    private static final String PROXY_PARAMETER = "proxy";
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("test-ca.pem")).getFile();
    private final String TEST_ROLE = "arn:aws:iam::123456789012:role/test-role";

    @Mock
    private ApacheHttpClient.Builder apacheHttpClientBuilder;
    @Mock
    private ApacheHttpClient apacheHttpClient;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    
    ObjectMapper objectMapper;

    @Test
    void testReadConnectionConfigurationDefault() throws JsonProcessingException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
        assertTrue(connectionConfiguration.isRequestCompressionEnabled());
    }

    @Test
    void testReadConnectionConfigurationES6Default() throws JsonProcessingException {
        final Map<String, Object> configMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, true, null, null, null, false);
        configMetadata.put(DISTRIBUTION_VERSION, "es6");
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertFalse(connectionConfiguration.isRequestCompressionEnabled());
    }

    @Test
    void testReadConnectionConfigurationAwsOptionServerlessDefault() throws JsonProcessingException {
        final String testArn = TEST_ROLE;
        final Map<String, Object> configMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, true, false, null, testArn, null, TEST_CERT_PATH, false, Collections.emptyMap());
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertTrue(connectionConfiguration.isServerless());
    }

    @Test
    void testCreateClientDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);
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
        configMetadata.put(SERVERLESS, true);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        when(apacheHttpClientBuilder.tlsTrustManagersProvider(any())).thenReturn(apacheHttpClientBuilder);
        when(apacheHttpClientBuilder.build()).thenReturn(apacheHttpClient);
        final OpenSearchClient openSearchClient;
        try (final MockedStatic<ApacheHttpClient> apacheHttpClientMockedStatic = mockStatic(ApacheHttpClient.class)) {
            apacheHttpClientMockedStatic.when(ApacheHttpClient::builder).thenReturn(apacheHttpClientBuilder);
            openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);
        }
        assertNotNull(openSearchClient);
        assertThat(openSearchClient._transport(), instanceOf(AwsSdk2Transport.class));
        assertThat(openSearchClient._transport().jsonpMapper(), instanceOf(PreSerializedJsonpMapper.class));
        verify(apacheHttpClientBuilder).tlsTrustManagersProvider(any());
        verify(apacheHttpClientBuilder).build();
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testReadConnectionConfigurationWithDeprecatedBasicCredentialsAndNoCert() throws JsonProcessingException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertEquals(TEST_USERNAME, connectionConfiguration.getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
        assertFalse(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testReadConnectionConfigurationWithBasicCredentialsAndNoCert() throws JsonProcessingException {
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        configurationMetadata.put("authentication", Map.of("username", TEST_USERNAME, "password", TEST_PASSWORD));
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertNotNull(connectionConfiguration.getAuthConfig());
        assertEquals(TEST_USERNAME, connectionConfiguration.getAuthConfig().getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getAuthConfig().getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
        assertFalse(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testReadConnectionConfigurationWithBothDeprecatedBasicCredentialsAndAuthConfigShouldThrow() throws JsonProcessingException {
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, null, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        configurationMetadata.put("authentication", Map.of("username", TEST_USERNAME, "password", TEST_PASSWORD));
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        assertFalse(openSearchSinkConfig.isAuthConfigValid());
    }

    @Test
    void testCreateClientWithDeprecatedBasicCredentialsAndNoCert() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateClientWithBasicCredentialsAndNoCert() throws IOException {
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        configurationMetadata.put("username", TEST_USERNAME);
        configurationMetadata.put("password", TEST_PASSWORD);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientNoCert() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);
        assertNotNull(openSearchClient);
        assertEquals(client.getLowLevelClient(), ((RestClientTransport) openSearchClient._transport()).restClient());
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateClientInsecure() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientInsecure() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);
        assertNotNull(openSearchClient);
        assertEquals(client.getLowLevelClient(), ((RestClientTransport) openSearchClient._transport()).restClient());
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateClientWithCertPath() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

  @Test
  void testCreateClientWithInsecureAndCertPath() throws IOException {
    // Insecure should take precedence over cert path when both are set
      final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
        TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, true);
    final ConnectionConfiguration connectionConfiguration =
        ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
    assertNull(connectionConfiguration.getCertPath());
    final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
    assertNotNull(client);
    client.close();
  }

  @Test
    void testCreateOpenSearchClientWithCertPath() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);
        assertNotNull(openSearchClient);
        assertEquals(client.getLowLevelClient(), ((RestClientTransport) openSearchClient._transport()).restClient());
        openSearchClient.shutdown();
        client.close();
    }

    @Test
    void testCreateClientWithAWSSigV4AndRegion() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, true, "us-west-2", null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-west-2", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testServerlessOptions() throws IOException {
        final String serverlessNetworkPolicyName = UUID.randomUUID().toString();
        final String serverlessCollectionName = UUID.randomUUID().toString();
        final String serverlessVpceId = UUID.randomUUID().toString();

        final Map<String, Object> metadata = new HashMap<>();
        final Map<String, Object> awsOptionMetadata = new HashMap<>();
        final Map<String, String> serverlessOptionsMetadata = new HashMap<>();
        serverlessOptionsMetadata.put("network_policy_name", serverlessNetworkPolicyName);
        serverlessOptionsMetadata.put("collection_name", serverlessCollectionName);
        serverlessOptionsMetadata.put("vpce_id", serverlessVpceId);
        awsOptionMetadata.put("region", UUID.randomUUID().toString());
        awsOptionMetadata.put("serverless", true);
        awsOptionMetadata.put("serverless_options", serverlessOptionsMetadata);
        awsOptionMetadata.put("sts_role_arn", TEST_ROLE);
        metadata.put("hosts", TEST_HOSTS);
        metadata.put("username", UUID.randomUUID().toString());
        metadata.put("password", UUID.randomUUID().toString());
        metadata.put("connect_timeout", 1);
        metadata.put("socket_timeout", 1);
        metadata.put("aws", awsOptionMetadata);

        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration = ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThat(connectionConfiguration.getServerlessNetworkPolicyName(), equalTo(serverlessNetworkPolicyName));
        assertThat(connectionConfiguration.getServerlessCollectionName(), equalTo(serverlessCollectionName));
        assertThat(connectionConfiguration.getServerlessVpceId(), equalTo(serverlessVpceId));
    }

    @Test
    void testCreateClientWithAWSSigV4DefaultRegion() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, true, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testCreateClientWithAWSSigV4AndInsecure() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, true, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testCreateClientWithAWSSigV4AndCertPath() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, true, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testCreateClientWithAWSSigV4AndSTSRole() throws JsonProcessingException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.getAwsRegion(), equalTo("us-east-1"));
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        connectionConfiguration.createClient(awsCredentialsSupplier);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getValue();

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(0));
    }

    @Test
    void testCreateOpenSearchClientWithAWSSigV4AndSTSRole() throws JsonProcessingException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(
                TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.getAwsRegion(), equalTo("us-east-1"));
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        final OpenSearchClient openSearchClient;
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);
        assertNotNull(openSearchClient);
        assertThat(openSearchClient._transport(), instanceOf(AwsSdk2Transport.class));
        final AwsSdk2Transport opensearchTransport = (AwsSdk2Transport) openSearchClient._transport();
        assertThat(opensearchTransport.options().credentials(), equalTo(awsCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier, times(2)).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getAllValues().get(1);

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(0));
    }

    @Test
    void testCreateClientWithAWSOption() throws JsonProcessingException {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final String testArn = TEST_ROLE;
        final String externalId = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, false, true, null, testArn, externalId, null,false, Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        connectionConfiguration.createClient(awsCredentialsSupplier);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getValue();

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getStsExternalId(), equalTo(externalId));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName1));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName1), equalTo(headerValue1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName2));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName2), equalTo(headerValue2));
    }

    @Test
    void testCreateOpenSearchClientWithAWSOption() throws JsonProcessingException {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final String testArn = TEST_ROLE;
        final String externalId = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, false, true, null, testArn, externalId, TEST_CERT_PATH, false, Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);

        assertNotNull(openSearchClient);
        assertThat(openSearchClient._transport(),  instanceOf(AwsSdk2Transport.class));
        final AwsSdk2Transport opensearchTransport = (AwsSdk2Transport) openSearchClient._transport();
        assertThat(opensearchTransport.options().credentials(), equalTo(awsCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier, times(2)).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getAllValues().get(1);

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName1));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName1), equalTo(headerValue1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName2));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName2), equalTo(headerValue2));
    }

    @Test
    void testCreateClientWithAWSSigV4AndHeaderOverrides() throws JsonProcessingException {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        configurationMetadata.put("aws_sts_header_overrides", Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        connectionConfiguration.createClient(awsCredentialsSupplier);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getValue();

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName1));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName1), equalTo(headerValue1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName2));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName2), equalTo(headerValue2));
    }

    @Test
    void testCreateOpenSearchClientWithAWSSigV4AndHeaderOverrides() throws JsonProcessingException {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        configurationMetadata.put("aws_sts_header_overrides", Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(configurationMetadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);


        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        final OpenSearchClient openSearchClient = connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier);

        assertNotNull(openSearchClient);
        assertThat(openSearchClient._transport(), instanceOf(AwsSdk2Transport.class));
        final AwsSdk2Transport opensearchTransport = (AwsSdk2Transport) openSearchClient._transport();
        assertThat(opensearchTransport.options().credentials(), equalTo(awsCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier, times(2)).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getAllValues().get(1);

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName1));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName1), equalTo(headerValue1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(headerName2));
        assertThat(actualOptions.getStsHeaderOverrides().get(headerName2), equalTo(headerValue2));
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_HostIP() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "121.121.121.121:80";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier));
        client.close();
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_HostName() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:80";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier));
        client.close();
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_SchemeProvided() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "http://example.com:4350";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier));
        client.close();
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_InvalidPort() throws JsonProcessingException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:port";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_NoPort() throws JsonProcessingException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_PortNotInRange() throws JsonProcessingException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:888888";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_NotHttp() throws JsonProcessingException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "socket://example.com:port";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfigByConfigMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithConnectionConfigurationBuilder_ProxyOptionalObjectShouldNotBeNull() throws IOException {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(TEST_HOSTS);
        final ConnectionConfiguration connectionConfiguration = builder.build();
        assertEquals(Optional.empty(), connectionConfiguration.getProxy());
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }
    
    private OpenSearchSinkConfig generateOpenSearchSinkConfig(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean awsSigv4, final String awsRegion,
            final String awsStsRoleArn, final String certPath, final boolean insecure) throws JsonProcessingException {
        
        final Map<String, Object> metadata = generateConfigurationMetadata(hosts, username, password, connectTimeout, socketTimeout, awsSigv4, awsRegion, awsStsRoleArn, certPath, insecure);
        return getOpenSearchSinkConfigByConfigMetadata(metadata);
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
            final String awsStsRoleArn, final String awsStsExternalId, final String certPath, final boolean insecure, Map<String, String> headerOverridesMap) {
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
        awsOptionMetadata.put("sts_external_id", awsStsExternalId);
        awsOptionMetadata.put("sts_header_overrides", headerOverridesMap);
        metadata.put("aws", awsOptionMetadata);
        metadata.put("cert", certPath);
        metadata.put("insecure", insecure);
        return metadata;
    }

    private OpenSearchSinkConfig getOpenSearchSinkConfigByConfigMetadata(final Map<String, Object> metadata) throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        String json = new ObjectMapper().writeValueAsString(metadata);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }
}