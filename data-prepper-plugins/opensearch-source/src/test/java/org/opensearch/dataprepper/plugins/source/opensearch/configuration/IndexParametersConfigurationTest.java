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
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class IndexParametersConfigurationTest {
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void index_parameter_configuration_test() throws JsonProcessingException {

        final String indexParameterConfigYaml =
                "  include:\n" +
                "    - \"includeTest\"\n" +
                "    - \"includeTest1\"\n" +
                "  exclude:\n" +
                "    - \"excludeTest\"";
        final IndexParametersConfiguration indexParametersConfiguration = objectMapper.readValue(indexParameterConfigYaml, IndexParametersConfiguration.class);
        assertThat(indexParametersConfiguration.getInclude(),equalTo(List.of("includeTest","includeTest1")));
        assertThat(indexParametersConfiguration.getExclude(),equalTo(List.of("excludeTest")));
    }
}
