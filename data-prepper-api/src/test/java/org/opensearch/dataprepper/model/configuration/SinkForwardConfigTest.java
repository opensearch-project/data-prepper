/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

public class SinkForwardConfigTest {

    @Test
    void testDefaults() {
        final SinkForwardConfig sinkForwardConfig = new SinkForwardConfig();
        assertThat(sinkForwardConfig.getPipelineNames(), nullValue());
        assertThat(sinkForwardConfig.getWithData(), nullValue());
        assertThat(sinkForwardConfig.getWithMetadata(), nullValue());
    }

    @Test
    void pipelines_lsit_with_one_pipeline_succeeds() {
        List<String> pipelines = List.of("pipeline1");
        Map<String, Object> withData = mock(Map.class);
        Map<String, Object> withMetadata = mock(Map.class);
        SinkForwardConfig sinkForwardConfig = new SinkForwardConfig(pipelines, withData, withMetadata);
        assertThat(sinkForwardConfig.getPipelineNames(), equalTo(pipelines));
        assertThat(sinkForwardConfig.getWithData(), equalTo(withData));
        assertThat(sinkForwardConfig.getWithMetadata(), equalTo(withMetadata));
    }

    @Test
    void pipelines_list_with_two_or_more_pipelines_throws_exception() {
        List<String> pipelines = List.of("pipeline1", "pipeline2");
        Map<String, Object> withData = mock(Map.class);
        Map<String, Object> withMetadata = mock(Map.class);
        assertThrows(InvalidPluginConfigurationException.class, ()->new SinkForwardConfig(pipelines, withData, withMetadata));
    }

    @Test
    void empty_pipelines_list_throws_exception() {
        assertThrows(InvalidPluginConfigurationException.class, ()->new SinkForwardConfig(List.of(), Map.of(), Map.of()));
    }
}

