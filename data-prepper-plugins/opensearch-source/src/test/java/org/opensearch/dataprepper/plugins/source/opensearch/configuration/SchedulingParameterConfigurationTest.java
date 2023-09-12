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

import java.time.Duration;
import java.time.Instant;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SchedulingParameterConfigurationTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void default_scheduling_configuration() {
        final SchedulingParameterConfiguration schedulingParameterConfiguration = new SchedulingParameterConfiguration();
        assertThat(schedulingParameterConfiguration.getCount(), equalTo(1));
        assertThat(schedulingParameterConfiguration.isStartTimeValid(), equalTo(true));
        assertThat(schedulingParameterConfiguration.getStartTime().isBefore(Instant.now()), equalTo(true));
        assertThat(schedulingParameterConfiguration.getInterval(), equalTo(Duration.ofHours(8)));
    }

    @Test
    void non_default_scheduling_configuration() throws JsonProcessingException {
        final String schedulingConfigurationYaml =
                "  count: 3\n" +
                "  start_time: \"2007-12-03T10:15:30.00Z\"\n";

        final SchedulingParameterConfiguration schedulingParameterConfiguration = objectMapper.readValue(schedulingConfigurationYaml, SchedulingParameterConfiguration.class);

        assertThat(schedulingParameterConfiguration.getCount(), equalTo(3));
        assertThat(schedulingParameterConfiguration.isStartTimeValid(), equalTo(true));
        assertThat(schedulingParameterConfiguration.getStartTime(), equalTo(Instant.parse("2007-12-03T10:15:30.00Z")));
    }

    @Test
    void invalid_start_time_configuration_test() throws JsonProcessingException {
        final String schedulingConfigurationYaml =
                "  count: 3\n" +
                        "  start_time: \"2007-12-03T10:15:30\"\n";

        final SchedulingParameterConfiguration schedulingParameterConfiguration = objectMapper.readValue(schedulingConfigurationYaml, SchedulingParameterConfiguration.class);

        assertThat(schedulingParameterConfiguration.getCount(), equalTo(3));
        assertThat(schedulingParameterConfiguration.isStartTimeValid(), equalTo(false));
    }
}
