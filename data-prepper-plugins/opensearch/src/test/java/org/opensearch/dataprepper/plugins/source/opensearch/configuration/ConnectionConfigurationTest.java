/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class ConnectionConfigurationTest {
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void default_connection_config() {
        final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();

        assertThat(connectionConfiguration.getConnectTimeout(), equalTo(null));
        assertThat(connectionConfiguration.getSocketTimeout(), equalTo(null));
        assertThat(connectionConfiguration.getCertPath(), equalTo(null));
        assertThat(connectionConfiguration.isInsecure(), equalTo(false));
    }
    @Test
    void connection_configuration_values_test() throws JsonProcessingException {

        final String connectionYaml =
                "  cert: \"cert\"\n" +
                "  insecure: true\n";
        final ConnectionConfiguration connectionConfig = objectMapper.readValue(connectionYaml, ConnectionConfiguration.class);
        assertThat(connectionConfig.getCertPath(),equalTo(Path.of("cert")));
        assertThat(connectionConfig.getSocketTimeout(),equalTo(null));
        assertThat(connectionConfig.getConnectTimeout(),equalTo(null));
        assertThat(connectionConfig.isInsecure(),equalTo(true));
    }

    @Test
    void connection_configuration_certificate_values_test() throws JsonProcessingException {

        final String connectionYaml =
                "  cert: \"cert\"\n" +
                "  certificate_content: \"certificate content\"\n" +
                "  insecure: true\n";
        final ConnectionConfiguration connectionConfig = objectMapper.readValue(connectionYaml, ConnectionConfiguration.class);
        assertThat(connectionConfig.getCertPath(),equalTo(Path.of("cert")));
        assertThat(connectionConfig.getCertificateContent(),equalTo("certificate content"));
        assertThat(connectionConfig.certificateFileAndContentAreMutuallyExclusive(), is(false));
        assertThat(connectionConfig.getSocketTimeout(),equalTo(null));
        assertThat(connectionConfig.getConnectTimeout(),equalTo(null));
        assertThat(connectionConfig.isInsecure(),equalTo(true));
    }
}
