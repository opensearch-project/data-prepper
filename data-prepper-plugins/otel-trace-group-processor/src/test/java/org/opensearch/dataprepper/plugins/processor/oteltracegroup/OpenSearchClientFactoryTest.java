package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltracegroup.ConnectionConfiguration2.PROXY;

@ExtendWith(MockitoExtension.class)
class OpenSearchClientFactoryTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("test-ca.pem")).getFile();
    private final String TEST_ROLE = "arn:aws:iam::123456789012:role/test-role";

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private OpenSearchClientFactory objectUnderTest;

    @Test
    void testcreateRestHighLevelClientDefault() throws IOException {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClientWithDeprecatedBasicCredentialsAndNoCert() throws IOException {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClientWithBasicCredentialsAndNoCert() throws IOException {
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        configurationMetadata.put("authentication", Map.of("username", TEST_USERNAME, "password", TEST_PASSWORD));
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                configurationMetadata, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClientInsecure() throws IOException {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, true);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClientWithCertPath() throws IOException {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClientWithAWSSigV4AndSTSRole() {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration2.class);
        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.getAwsRegion(), equalTo("us-east-1"));
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));
        assertThat(connectionConfiguration.getAwsStsRoleArn(), equalTo(TEST_ROLE));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(awsCredentialsOptionsArgumentCaptor.capture());
        final AwsCredentialsOptions actualOptions = awsCredentialsOptionsArgumentCaptor.getValue();

        assertThat(actualOptions.getStsRoleArn(), equalTo(TEST_ROLE));
        assertThat(actualOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(actualOptions.getStsHeaderOverrides().size(), equalTo(0));
    }

    @Test
    void testcreateRestHighLevelClientWithAWSOption() {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final String testArn = TEST_ROLE;
        final String externalId = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, false, true, null, testArn, externalId, null,false, Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                configurationMetadata, ConnectionConfiguration2.class);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);

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
    void testcreateRestHighLevelClientWithAWSSigV4AndHeaderOverrides() {
        final String headerName1 = UUID.randomUUID().toString();
        final String headerValue1 = UUID.randomUUID().toString();
        final String headerName2 = UUID.randomUUID().toString();
        final String headerValue2 = UUID.randomUUID().toString();
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(TEST_HOSTS, null, null, null, null, true, null, TEST_ROLE, TEST_CERT_PATH, false);
        configurationMetadata.put("aws_sts_header_overrides", Map.of(headerName1, headerValue1, headerName2, headerValue2));
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                configurationMetadata, ConnectionConfiguration2.class);

        assertThat(connectionConfiguration, notNullValue());
        assertThat(connectionConfiguration.isAwsSigv4(), equalTo(true));

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);

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
    void testcreateRestHighLevelClient_WithValidHttpProxy_HostIP() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "121.121.121.121:80";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        assertEquals(connectionConfiguration.getProxy(), testHttpProxy);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClient_WithValidHttpProxy_HostName() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:80";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        assertEquals(connectionConfiguration.getProxy(), testHttpProxy);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClient_WithValidHttpProxy_SchemeProvided() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "http://example.com:4350";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        assertEquals(connectionConfiguration.getProxy(), testHttpProxy);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        final RestHighLevelClient client = objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier);
        assertNotNull(client);
        client.close();
    }

    @Test
    void testcreateRestHighLevelClient_WithInvalidHttpProxy_InvalidPort() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:port";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        assertEquals(connectionConfiguration.getProxy(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier));
    }

    @Test
    void testcreateRestHighLevelClient_WithInvalidHttpProxy_NoPort() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier));
    }

    @Test
    void testcreateRestHighLevelClient_WithInvalidHttpProxy_PortNotInRange() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:888888";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier));
    }

    @Test
    void testcreateRestHighLevelClient_WithInvalidHttpProxy_NotHttp() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "socket://example.com:port";
        metadata.put(PROXY, testHttpProxy);
        final ConnectionConfiguration2 connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration2.class);
        assertEquals(connectionConfiguration.getProxy(), testHttpProxy);
        objectUnderTest = OpenSearchClientFactory.fromConnectionConfiguration(connectionConfiguration);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.createRestHighLevelClient(awsCredentialsSupplier));
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
}