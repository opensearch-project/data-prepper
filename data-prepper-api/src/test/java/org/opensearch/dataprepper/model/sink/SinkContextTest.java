/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
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
        Record<Event> record = mock(Record.class);
        EventMetadata eventMetadata = mock(EventMetadata.class);
        Event event = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        doNothing().when(eventHandle).acquireReference();
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(record.getData()).thenReturn(event);
        when(event.getMetadata()).thenReturn(eventMetadata);
        Collection<Record<Event>> records = Collections.singletonList(record);
        SinkForwardRecordsContext sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);

        assertThat(sinkContext.forwardRecords(sinkForwardRecordsContext, Map.of("datakey1", "datavalue1"), Map.of("metadataKey1", "metadataValue1")), equalTo(true));
        verify(forwardPipeline1, times(0)).sendEvents(eq(records));
        verify(forwardPipeline2, times(0)).sendEvents(eq(records));
        sinkForwardRecordsContext.addRecords(records);
        assertThat(sinkContext.forwardRecords(sinkForwardRecordsContext, Map.of("datakey1", "datavalue1"), Map.of("metadataKey1", "metadataValue1")), equalTo(true));
        verify(forwardPipeline1, times(1)).sendEvents(eq(records));
        verify(forwardPipeline2, times(1)).sendEvents(eq(records));
        verify(event, times(1)).put(any(String.class), any(Object.class));
        verify(event, times(1)).getMetadata();
        verify(eventMetadata, times(1)).setAttribute(any(String.class), any(Object.class));
        assertThat(sinkContext.forwardRecords(sinkForwardRecordsContext, null, null), equalTo(true));
        verify(forwardPipeline1, times(2)).sendEvents(eq(records));
        verify(forwardPipeline2, times(2)).sendEvents(eq(records));
        assertThat(sinkContext.forwardRecords(sinkForwardRecordsContext, Map.of(), Map.of()), equalTo(true));
        verify(forwardPipeline1, times(3)).sendEvents(eq(records));
        verify(forwardPipeline2, times(3)).sendEvents(eq(records));
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
        SinkForwardRecordsContext sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        Record<Event> record = mock(Record.class);
        Event event = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        doNothing().when(eventHandle).acquireReference();
        when(record.getData()).thenReturn(event);
        when(event.getEventHandle()).thenReturn(eventHandle);
        sinkForwardRecordsContext.addRecords(List.of(record));
        assertThat(sinkContext.forwardRecords(sinkForwardRecordsContext, Map.of(), Map.of()), equalTo(false));
    }

    @Test
    public void testWithNoForwardToPipelines() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        final List<String> testForwardToPipelineNames = Collections.emptyList();
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes, testIncludeKeys, testExcludeKeys, testForwardToPipelineNames);
        Record<Event> record = mock(Record.class);
        Event event = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        doNothing().when(eventHandle).acquireReference();
        when(record.getData()).thenReturn(event);
        when(event.getEventHandle()).thenReturn(eventHandle);
        
        SinkForwardRecordsContext sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        sinkForwardRecordsContext.addRecords(List.of(record));
        assertThat(sinkContext.forwardRecords(sinkForwardRecordsContext, Map.of(), Map.of()), equalTo(false));
    }
}

