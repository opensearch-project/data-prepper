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

import java.time.Duration;
import java.time.LocalDateTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SchedulingParameterConfigurationTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void scheduling_parameter_configuration_test() throws JsonProcessingException {

        final String schedulingParameterYaml =
                "  rate: \"P2DT3H4M\"\n" +
                "  job_count: 3\n" +
                "  start_time: 2023-05-05T18:00:00\n";
        final SchedulingParameterConfiguration schedulingParameterConfiguration = objectMapper.readValue(schedulingParameterYaml, SchedulingParameterConfiguration.class);
        assertThat(schedulingParameterConfiguration.getRate(),equalTo(Duration.parse("P2DT3H4M")));
        assertThat(schedulingParameterConfiguration.getJobCount(),equalTo(3));
        assertThat(schedulingParameterConfiguration.getStartTime(),equalTo(LocalDateTime.parse("2023-05-05T18:00:00")));
    }
}
