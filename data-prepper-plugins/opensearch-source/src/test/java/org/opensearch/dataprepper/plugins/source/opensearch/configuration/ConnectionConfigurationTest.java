/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.Test;

import java.nio.file.Path;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConnectionConfigurationTest {
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void connection_configuration_values_test() throws JsonProcessingException {

        final String connectionYaml = "  hosts: [\"http://localhost:9200\"]\n" +
                "  username: test\n" +
                "  password: test\n" +
                "  cert: \"cert\"\n" +
                "  insecure: true\n" +
                "  socket_timeout: 500\n" +
                "  connection_timeout: 500";
        final ConnectionConfiguration connectionConfig = objectMapper.readValue(connectionYaml, ConnectionConfiguration.class);
        assertThat(connectionConfig.getHosts().get(0),equalTo("http://localhost:9200"));
        assertThat(connectionConfig.getUsername(),equalTo("test"));
        assertThat(connectionConfig.getPassword(),equalTo("test"));
        assertThat(connectionConfig.getCertPath(),equalTo(Path.of("cert")));
        assertThat(connectionConfig.getSocketTimeout(),equalTo(500));
        assertThat(connectionConfig.getConnectTimeout(),equalTo(500));
    }
}
