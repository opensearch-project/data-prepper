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

public class QueryParameterConfigurationTest {


    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void query_params_config_values_test() throws JsonProcessingException {
        final String queryConfigurationYaml =
                "  fields: [\"test_variable : test_value\"]";
        final QueryParameterConfiguration queryParameterConfiguration = objectMapper.readValue(queryConfigurationYaml, QueryParameterConfiguration.class);
        assertThat(queryParameterConfiguration.getFields(),equalTo(List.of("test_variable : test_value")));
    }
}
