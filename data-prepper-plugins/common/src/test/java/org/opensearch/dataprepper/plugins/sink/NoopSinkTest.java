package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;

import java.util.List;

public class NoopSinkTest {

    private NoopSink noopSink;

    @Mock
    private Record<Object> mockRecord;

    @Mock
    private Event mockEvent;

    @Mock
    private EventHandle mockEventHandle;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        noopSink = new NoopSink();
    }

    @Test
    public void testOutput_releasesEventHandles() {
        // Arrange
        List<Record<Object>> records = List.of(mockRecord);
        when(mockRecord.getData()).thenReturn(mockEvent);
        when(mockEvent.getEventHandle()).thenReturn(mockEventHandle);

        // Act
        noopSink.output(records);

        // Assert
        verify(mockEventHandle, times(1)).release(true);
    }

    @Test
    public void testOutput_multipleRecords_releasesAllEventHandles() {
        // Arrange
        Record<Object> mockRecord2 = mock(Record.class);
        Event mockEvent2 = mock(Event.class);
        EventHandle mockEventHandle2 = mock(EventHandle.class);

        List<Record<Object>> records = List.of(mockRecord, mockRecord2);

        when(mockRecord.getData()).thenReturn(mockEvent);
        when(mockEvent.getEventHandle()).thenReturn(mockEventHandle);

        when(mockRecord2.getData()).thenReturn(mockEvent2);
        when(mockEvent2.getEventHandle()).thenReturn(mockEventHandle2);

        // Act
        noopSink.output(records);

        // Assert
        verify(mockEventHandle, times(1)).release(true);
        verify(mockEventHandle2, times(1)).release(true);
    }
}
