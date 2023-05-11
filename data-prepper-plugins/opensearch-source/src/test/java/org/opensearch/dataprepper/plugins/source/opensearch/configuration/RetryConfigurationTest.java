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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RetryConfigurationTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void retry_config_values_test() throws JsonProcessingException {
        final String retryConfigurationYaml =
                "  max_retries: 3";
        final RetryConfiguration retryConfiguration = objectMapper.readValue(retryConfigurationYaml, RetryConfiguration.class);
        assertThat(retryConfiguration.getMaxRetries(),equalTo(3));
    }
}
