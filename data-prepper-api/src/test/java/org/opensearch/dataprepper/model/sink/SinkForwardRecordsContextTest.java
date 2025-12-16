/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;

import static org.mockito.Mockito.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

public class SinkForwardRecordsContextTest {
    
    SinkForwardRecordsContext sinkForwardRecordsContext;

    @Test
    public void testSinkForwardRecordContextBasic() {
        SinkContext sinkContext = mock(SinkContext.class);
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of());
        sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        Record<Event> record1 = mock(Record.class);
        Record<Event> record2 = mock(Record.class);
        Record<Event> record3 = mock(Record.class);
        sinkForwardRecordsContext.addRecord(record1);
        sinkForwardRecordsContext.addRecords(List.of(record2, record3));
        List<Record<Event>> records = sinkForwardRecordsContext.getRecords();
        assertThat(records.size(), equalTo(0));
    }

    @Test
    public void testSinkForwardRecordContextWithForwardingPipelines() {
        SinkContext sinkContext = mock(SinkContext.class);
        HeadlessPipeline headlessPipeline = mock(HeadlessPipeline.class);
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of("pipeline1", headlessPipeline));
        sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        Record<Event> record1 = mock(Record.class);
        Record<Event> record2 = mock(Record.class);
        Record<Event> record3 = mock(Record.class);
        sinkForwardRecordsContext.addRecord(record1);
        sinkForwardRecordsContext.addRecords(List.of(record2, record3));
        List<Record<Event>> records = sinkForwardRecordsContext.getRecords();
        assertThat(records.size(), equalTo(3));
        sinkForwardRecordsContext.clearRecords();
        assertThat(sinkForwardRecordsContext.getRecords().size(), equalTo(0));
    }

    @Test
    public void testSinkForwardRecordContextClearRecords() {
        SinkContext sinkContext = mock(SinkContext.class);
        HeadlessPipeline headlessPipeline = mock(HeadlessPipeline.class);
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of("pipeline1", headlessPipeline));
        sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        Record<Event> record1 = mock(Record.class);
        Record<Event> record2 = mock(Record.class);
        sinkForwardRecordsContext.addRecords(List.of(record1, record2));
        assertThat(sinkForwardRecordsContext.getRecords().size(), equalTo(2));
        sinkForwardRecordsContext.clearRecords();
        assertThat(sinkForwardRecordsContext.getRecords().size(), equalTo(0));
    }
}
