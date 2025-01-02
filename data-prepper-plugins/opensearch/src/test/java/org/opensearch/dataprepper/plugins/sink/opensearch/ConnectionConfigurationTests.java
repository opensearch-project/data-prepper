/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

@ExtendWith(MockitoExtension.class)
class ConnectionConfigurationTests {
    private static final String OPEN_SEARCH_SINK_CONFIGURATIONS = "open-search-sink-configurations.yaml";
    private static final String EMPTY_SINK_CONFIG = "empty-sink";
    private static final String ES6_DEFAULT_CONFIG = "es6-default";
    private static final String AWS_SERVERLESS_DEFAULT = "aws-serverless-default";
    private static final String AWS_SERVERLESS_NO_CERT = "aws-serverless-no-cert";
    private static final String BASIC_CREDENTIALS_NO_CERT = "basic-credentials-no-cert";
    private static final String BASIC_CREDENTIALS_NO_CERT_INSECURE = "basic-credentials-no-cert-insecure";
    private static final String BASIC_CREDENTIALS_WITH_CERT = "basic-credentials-with-cert";
    private static final String AWS_REGION_ONLY = "aws-region-only";
    private static final String SERVERLESS_OPTIONS = "serverless-options";
    private static final String AWS_REGION_ONLY_INSECURE = "aws-region-only-insecure";
    private static final String AWS_WITH_CERT = "aws-with-cert";
    private static final String AWS_WITH_CERT_AND_ARN = "aws-with-cert-and-arn";
    private static final String AWS_WITH_2_HEADER = "aws-with-2-header";
    private static final String VALID_PROXY_IP = "valid-proxy-ip";
    private static final String VALID_PROXY_HOST_NAME = "valid-proxy-host-name";
    private static final String VALID_HTTP_PROXY_SCHEME = "valid-http-proxy-scheme";
    private static final String INVALID_HTTP_PROXY_PORT = "invalid-http-proxy-port";
    private static final String INVALID_HTTP_PROXY_NO_PORT = "invalid-http-proxy-no-port";
    private static final String INVALID_HTTP_PROXY_PORT_NOT_IN_RANGE = "invalid-http-proxy-port-not-in-range";
    private static final String INVALID_HTTP_PROXY_NOT_HTTP = "invalid-http-proxy-not-http";

    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "test-username";
    private final String TEST_PASSWORD = "test-password";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_ROLE = "arn:aws:iam::123456789012:role/test-role";
    private final String TEST_NETWORK_POLICY = "test network policy";
    private final String TEST_COLLECTION_NAME = "test collection";
    private final String TEST_VPCE_ID = "test vpce id";
    private final String TEST_EXTERNAL_ID = "test-external-id";
    private final String TEST_HEADER_NAME_1 = "header1";
    private final String TEST_HEADER_NAME_2 = "header2";
    private final String TEST_HEADER_VALUE_1 = "test-header-1";
    private final String TEST_HEADER_VALUE_2 = "test-header-2";

    @Mock
    private ApacheHttpClient.Builder apacheHttpClientBuilder;
    @Mock
    private ApacheHttpClient apacheHttpClient;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    ObjectMapper objectMapper;

    @Test
    void testReadConnectionConfigurationDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(EMPTY_SINK_CONFIG);
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
    void testReadConnectionConfigurationES6Default() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(ES6_DEFAULT_CONFIG);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertFalse(connectionConfiguration.isRequestCompressionEnabled());
    }

    @Test
    void testReadConnectionConfigurationAwsOptionServerlessDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_SERVERLESS_DEFAULT);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertTrue(connectionConfiguration.isServerless());
    }

    @Test
    void testCreateClientDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(EMPTY_SINK_CONFIG);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(EMPTY_SINK_CONFIG);
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
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_SERVERLESS_NO_CERT);
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
    void testReadConnectionConfigurationWithBasicCredentialsAndNoCert() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_NO_CERT);
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
    void testCreateClientWithBasicCredentialsAndNoCert() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_NO_CERT);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }


    @Test
    void testCreateOpenSearchClientNoCert() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_NO_CERT);
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
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_NO_CERT_INSECURE);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientInsecure() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_NO_CERT_INSECURE);
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
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_WITH_CERT);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testCreateOpenSearchClientWithCertPath() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(BASIC_CREDENTIALS_WITH_CERT);
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
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_REGION_ONLY);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-east-2", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testServerlessOptions() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(SERVERLESS_OPTIONS);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThat(connectionConfiguration.getServerlessNetworkPolicyName(), equalTo(TEST_NETWORK_POLICY));
        assertThat(connectionConfiguration.getServerlessCollectionName(), equalTo(TEST_COLLECTION_NAME));
        assertThat(connectionConfiguration.getServerlessVpceId(), equalTo(TEST_VPCE_ID));
    }

    @Test
    void testCreateClientWithAWSSigV4AndInsecure() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_REGION_ONLY_INSECURE);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testCreateClientWithAWSSigV4AndCertPath() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_CERT);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testCreateClientWithAWSSigV4AndSTSRole() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_CERT_AND_ARN);
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
    void testCreateOpenSearchClientWithAWSSigV4AndSTSRole() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_CERT_AND_ARN);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);

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
    void testCreateClientWithAWSOption() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_2_HEADER);
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
        assertThat(actualOptions.getStsExternalId(), equalTo(TEST_EXTERNAL_ID));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_1), equalTo(TEST_HEADER_VALUE_1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_2), equalTo(TEST_HEADER_VALUE_2));
    }

    @Test
    void testCreateOpenSearchClientWithAWSOption() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_2_HEADER);
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
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_1), equalTo(TEST_HEADER_VALUE_1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_2), equalTo(TEST_HEADER_VALUE_2));
    }

    @Test
    void testCreateClientWithAWSSigV4AndHeaderOverrides() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_2_HEADER);
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
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_1), equalTo(TEST_HEADER_VALUE_1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_2), equalTo(TEST_HEADER_VALUE_2));
    }

    @Test
    void testCreateOpenSearchClientWithAWSSigV4AndHeaderOverrides() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(AWS_WITH_2_HEADER);
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
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_1), equalTo(TEST_HEADER_VALUE_1));
        assertThat(actualOptions.getStsHeaderOverrides(), hasKey(TEST_HEADER_NAME_2));
        assertThat(actualOptions.getStsHeaderOverrides().get(TEST_HEADER_NAME_2), equalTo(TEST_HEADER_VALUE_2));
    }

    @Test
    void testCreateAllClients_WithValidHttpProxy_HostIP() throws IOException {
        final String testHttpProxy = "121.121.121.121:80";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(VALID_PROXY_IP);
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
        final String testHttpProxy = "example.com:80";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(VALID_PROXY_HOST_NAME);
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
        final String testHttpProxy = "http://example.com:4350";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(VALID_HTTP_PROXY_SCHEME);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier);
        assertNotNull(client);
        assertNotNull(connectionConfiguration.createOpenSearchClient(client, awsCredentialsSupplier));
        client.close();
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_InvalidPort() throws IOException {
        final String testHttpProxy = "example.com:port";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(INVALID_HTTP_PROXY_PORT);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_NoPort() throws IOException {
        final String testHttpProxy = "example.com";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(INVALID_HTTP_PROXY_NO_PORT);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_PortNotInRange() throws IOException {
        final String testHttpProxy = "example.com:888888";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(INVALID_HTTP_PROXY_PORT_NOT_IN_RANGE);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient(awsCredentialsSupplier));
    }

    @Test
    void testCreateClient_WithInvalidHttpProxy_NotHttp() throws IOException {
        final String testHttpProxy = "socket://example.com:port";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(INVALID_HTTP_PROXY_NOT_HTTP);
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

    private OpenSearchSinkConfig generateOpenSearchSinkConfig(String pipelineName) throws IOException {
        final File configurationFile = new File(getClass().getClassLoader().getResource(OPEN_SEARCH_SINK_CONFIGURATIONS).getFile());
        objectMapper = new ObjectMapper(new YAMLFactory());
        final Map<String, Object> pipelineConfigs = objectMapper.readValue(configurationFile, Map.class);
        final Map<String, Object> pipelineConfig = (Map<String, Object>) pipelineConfigs.get(pipelineName);
        final Map<String, Object> sinkMap = (Map<String, Object>) pipelineConfig.get("sink");
        final Map<String, Object> opensearchSinkMap = (Map<String, Object>) sinkMap.get("opensearch");
        String json = objectMapper.writeValueAsString(opensearchSinkMap);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }
}