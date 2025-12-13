/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

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
    void testCustomValues() {
        List<String> pipelines = List.of("pipeline1");
        Map<String, Object> withData = mock(Map.class);
        Map<String, Object> withMetadata = mock(Map.class);
        SinkForwardConfig sinkForwardConfig = new SinkForwardConfig(pipelines, withData, withMetadata);
        assertThat(sinkForwardConfig.getPipelineNames(), equalTo(pipelines));
        assertThat(sinkForwardConfig.getWithData(), equalTo(withData));
        assertThat(sinkForwardConfig.getWithMetadata(), equalTo(withMetadata));
    }

    @Test
    void testInvalidValues() {
        List<String> pipelines = List.of("pipeline1", "pipeline2");
        Map<String, Object> withData = mock(Map.class);
        Map<String, Object> withMetadata = mock(Map.class);
        assertThrows(RuntimeException.class, ()->new SinkForwardConfig(pipelines, withData, withMetadata));
    }
}

