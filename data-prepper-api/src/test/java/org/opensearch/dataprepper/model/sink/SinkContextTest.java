/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SinkContextTest {
    private SinkContext sinkContext;

    @Test
    public void testSinkContextBasic() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        final List<String> testForwardToPipelineNames = Collections.emptyList();
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes, testIncludeKeys, testExcludeKeys, testForwardToPipelineNames);
        assertThat(sinkContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(sinkContext.getRoutes(), equalTo(testRoutes));
        assertThat(sinkContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(sinkContext.getExcludeKeys(), equalTo(testExcludeKeys));
        assertThat(sinkContext.getForwardToPipelines(), equalTo(Collections.emptyMap()));

    }

    @Test
    public void testSinkContextWithTagsOnly() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        sinkContext = new SinkContext(testTagsTargetKey);
        assertThat(sinkContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(sinkContext.getRoutes(), equalTo(null));
        assertThat(sinkContext.getIncludeKeys(), equalTo(null));
        assertThat(sinkContext.getExcludeKeys(), equalTo(null));

    }

    @Test
    public void testForwardToPipelinesWithPipelineMap() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        final List<String> testForwardToPipelineNames = List.of("forward-pipeline1", "forward-pipeline2");
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes, testIncludeKeys, testExcludeKeys, testForwardToPipelineNames);
        Map<String, HeadlessPipeline> forwardToPipelines = sinkContext.getForwardToPipelines();
        assertThat(forwardToPipelines.get("forward-pipeline1"), equalTo(null));
        assertThat(forwardToPipelines.get("forward-pipeline2"), equalTo(null));
        HeadlessPipeline forwardPipeline1 = mock(HeadlessPipeline.class);
        HeadlessPipeline forwardPipeline2 = mock(HeadlessPipeline.class);
        sinkContext.setForwardToPipelines(Map.of("forward-pipeline1", forwardPipeline1, "forward-pipeline2", forwardPipeline2));
        forwardToPipelines = sinkContext.getForwardToPipelines();
        assertThat(forwardToPipelines.get("forward-pipeline1"), equalTo(forwardPipeline1));
        assertThat(forwardToPipelines.get("forward-pipeline2"), equalTo(forwardPipeline2));
        Collection<Record<Event>> records = mock(Collection.class);
        assertThat(sinkContext.forwardRecords(records), equalTo(true));
        verify(forwardPipeline1, times(1)).sendEvents(eq(records));
        verify(forwardPipeline2, times(1)).sendEvents(eq(records));
    }

    @Test
    public void testForwardToPipelinesWithPipelineMapAndFailureCases() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        final List<String> testForwardToPipelineNames = List.of("forward-pipeline1", "forward-pipeline2");
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes, testIncludeKeys, testExcludeKeys, testForwardToPipelineNames);
        Map<String, HeadlessPipeline> forwardToPipelines = sinkContext.getForwardToPipelines();
        assertThat(forwardToPipelines.get("forward-pipeline1"), equalTo(null));
        assertThat(forwardToPipelines.get("forward-pipeline2"), equalTo(null));
        HeadlessPipeline forwardPipeline1 = mock(HeadlessPipeline.class);
        assertThrows(RuntimeException.class, () -> sinkContext.setForwardToPipelines(Map.of("forward-pipeline1", forwardPipeline1)));
        Collection<Record<Event>> records = mock(Collection.class);
        assertThat(sinkContext.forwardRecords(records), equalTo(false));
    }

    @Test
    public void testWithNoForwardToPipelines() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        final List<String> testForwardToPipelineNames = Collections.emptyList();
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes, testIncludeKeys, testExcludeKeys, testForwardToPipelineNames);
        Collection<Record<Event>> records = mock(Collection.class);
        assertThat(sinkContext.forwardRecords(records), equalTo(false));
    }
}

