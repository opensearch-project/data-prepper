package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionConfigurationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final String TEST_PIPELINE_NAME = "Test-Pipeline";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("test-ca.pem")).getFile();
    private final String TEST_ROLE = "arn:aws:iam::123456789012:role/test-role";

    @Test
    void testDeserializeConnectionConfigurationDefault() {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration.class);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
    }

    @Test
    void testDeserializeConnectionConfigurationWithAwsSigV4() {
        final String stsRoleArn = "arn:aws:iam::123456789012:role/TestRole";
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, null, null, null, null, true, null, stsRoleArn, null, false);
        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration.class);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
        assertTrue(connectionConfiguration.isValidAwsAuth());
        assertTrue(connectionConfiguration.isValidStsRoleArn());
    }

    @Test
    void testDeserializeConnectionConfigurationAwsOptionServerlessDefault() {
        final String testArn = TEST_ROLE;
        final Map<String, Object> configMetadata = generateConfigurationMetadataWithAwsOption(TEST_HOSTS, null, null, null, null, true, false, null, testArn, null, TEST_CERT_PATH, false, Collections.emptyMap());
        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                configMetadata, ConnectionConfiguration.class);
        assertTrue(connectionConfiguration.isValidAwsAuth());
        assertTrue(connectionConfiguration.getAwsOption().isServerless());
        assertTrue(connectionConfiguration.getAwsOption().isValidStsRoleArn());
    }

    @Test
    void testDeserializeConnectionConfigurationWithDeprecatedBasicCredentialsAndNoCert() {
        final Map<String, Object> pluginSetting = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                pluginSetting, ConnectionConfiguration.class);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertEquals(TEST_USERNAME, connectionConfiguration.getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
        assertFalse(connectionConfiguration.isAwsSigv4());
    }

    @Test
    void testDeserializeConnectionConfigurationWithBasicCredentialsAndNoCert() {
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(
                TEST_HOSTS, null, null, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        configurationMetadata.put("authentication", Map.of("username", TEST_USERNAME, "password", TEST_PASSWORD));
        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                configurationMetadata, ConnectionConfiguration.class);
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
    void testDeserializeConnectionConfigurationWithBothDeprecatedBasicCredentialsAndAuthConfig() {
        final Map<String, Object> configurationMetadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, null, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        configurationMetadata.put("authentication", Map.of("username", TEST_USERNAME, "password", TEST_PASSWORD));
        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                configurationMetadata, ConnectionConfiguration.class);
        assertFalse(connectionConfiguration.isValidAuthentication());
    }

    @Test
    void testServerlessOptions() {
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

        final ConnectionConfiguration connectionConfiguration = OBJECT_MAPPER.convertValue(
                metadata, ConnectionConfiguration.class);
        assertThat(connectionConfiguration.getAwsOption().isServerless(), is(true));
        assertThat(connectionConfiguration.getAwsOption().getServerlessOptions().getNetworkPolicyName(),
                equalTo(serverlessNetworkPolicyName));
        assertThat(connectionConfiguration.getAwsOption().getServerlessOptions().getCollectionName(),
                equalTo(serverlessCollectionName));
        assertThat(connectionConfiguration.getAwsOption().getServerlessOptions().getVpceId(),
                equalTo(serverlessVpceId));
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