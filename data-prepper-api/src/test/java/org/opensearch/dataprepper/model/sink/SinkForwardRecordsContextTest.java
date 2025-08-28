/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;

import static org.mockito.Mockito.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.util.List;

public class SinkForwardRecordsContextTest {
    
    SinkForwardRecordsContext sinkForwardRecordsContext;

    @Test
    public void testSinkForwardRecordContextBasic() {
        sinkForwardRecordsContext = new SinkForwardRecordsContext();
        Event event = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        doNothing().when(eventHandle).acquireReference();
        Record<Event> record1 = mock(Record.class);
        Record<Event> record2 = mock(Record.class);
        Record<Event> record3 = mock(Record.class);
        when(record1.getData()).thenReturn(event);
        when(record2.getData()).thenReturn(event);
        when(record3.getData()).thenReturn(event);
        when(event.getEventHandle()).thenReturn(eventHandle);
        sinkForwardRecordsContext.addRecord(record1);
        sinkForwardRecordsContext.addRecords(List.of(record2, record3));
        List<Record<Event>> records = sinkForwardRecordsContext.getRecords();
        assertThat(records.size(), equalTo(3));
    }
}

