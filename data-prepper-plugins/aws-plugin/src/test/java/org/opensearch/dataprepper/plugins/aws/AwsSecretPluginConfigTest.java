package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsSecretPluginConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    @Test
    void testDefault() {
        final AwsSecretPluginConfig awsSecretPluginConfig = new AwsSecretPluginConfig();
        assertThat(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap(), equalTo(Collections.emptyMap()));
    }

    @Test
    void testDeserialization() throws IOException {
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-plugin-config.yaml");
        final AwsSecretPluginConfig awsSecretPluginConfig = OBJECT_MAPPER.readValue(
                inputStream, AwsSecretPluginConfig.class);
        final Map<String, AwsSecretManagerConfiguration> awsSecretManagerConfigurationMap =
                awsSecretPluginConfig.getAwsSecretManagerConfigurationMap();
        assertThat(awsSecretManagerConfigurationMap.size(), equalTo(1));
        assertThat(awsSecretManagerConfigurationMap.get("test_config"),
                instanceOf(AwsSecretManagerConfiguration.class));
    }
}