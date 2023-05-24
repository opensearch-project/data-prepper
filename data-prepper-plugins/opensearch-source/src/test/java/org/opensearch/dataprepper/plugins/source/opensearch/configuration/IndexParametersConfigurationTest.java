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

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class IndexParametersConfigurationTest {
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void default_index_parameters_configuration() {
        final IndexParametersConfiguration indexParametersConfiguration = new IndexParametersConfiguration();

        assertThat(indexParametersConfiguration.getExcludePatterns(), equalTo(Collections.emptyList()));
        assertThat(indexParametersConfiguration.getIncludedIndices(), equalTo(Collections.emptyList()));

        assertThat(indexParametersConfiguration.isExcludeRegexValid(), equalTo(true));
        assertThat(indexParametersConfiguration.getExcludePatterns(), equalTo(Collections.emptyList()));
    }

    @Test
    void index_parameter_configuration_test() throws JsonProcessingException {

        final String indexParameterConfigYaml =
                "  include:\n" +
                "    - index_name_regex: \"includeTest\"\n" +
                "    - index_name_regex: \"includeTest1\"\n" +
                "  exclude:\n" +
                "    - \"excludeTest\"";
        final IndexParametersConfiguration indexParametersConfiguration = objectMapper.readValue(indexParameterConfigYaml, IndexParametersConfiguration.class);

        assertThat(indexParametersConfiguration.getIncludedIndices(), notNullValue());
        assertThat(indexParametersConfiguration.getIncludedIndices().size(), equalTo(2));
        assertThat(indexParametersConfiguration.isExcludeRegexValid(), equalTo(true));
        assertThat(indexParametersConfiguration.getExcludePatterns(), notNullValue());
        assertThat(indexParametersConfiguration.getExcludePatterns().size(), equalTo(1));
        assertThat(indexParametersConfiguration.getExcludePatterns().get(0).pattern(), equalTo("excludeTest"));
    }

    @Test
    void invalid_exclude_regex_returns_false() throws JsonProcessingException {
        final String indexParameterConfigYaml =
                "  include:\n" +
                        "    - index_name_regex: \"includeTest\"\n" +
                        "    - index_name_regex: \"includeTest1\"\n" +
                        "  exclude:\n" +
                        "    - \"[\"";
        final IndexParametersConfiguration indexParametersConfiguration = objectMapper.readValue(indexParameterConfigYaml, IndexParametersConfiguration.class);

        assertThat(indexParametersConfiguration.isExcludeRegexValid(), equalTo(false));
    }
}
