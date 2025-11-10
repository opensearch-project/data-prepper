/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataStreamIndexTest {
    
    @Mock
    private DataStreamDetector dataStreamDetector;
    
    @Mock
    private IndexConfiguration indexConfiguration;
    
    @Mock
    private Event event;
    
    @Mock
    private EventHandle eventHandle;
    
    private DataStreamIndex dataStreamIndex;
    
    @BeforeEach
    void setUp() {
        dataStreamIndex = new DataStreamIndex(dataStreamDetector, indexConfiguration);
    }
    
    @Test
    void determineAction_returnsCreate_whenIndexIsDataStream() {
        when(dataStreamDetector.isDataStream("my-data-stream")).thenReturn(true);
        
        String result = dataStreamIndex.determineAction("index", "my-data-stream");
        
        assertThat(result, equalTo("create"));
    }
    
    @Test
    void determineAction_returnsConfiguredAction_whenIndexIsNotDataStream() {
        when(dataStreamDetector.isDataStream("regular-index")).thenReturn(false);
        
        String result = dataStreamIndex.determineAction("update", "regular-index");
        
        assertThat(result, equalTo("update"));
    }
    
    @Test
    void determineAction_returnsIndexAsDefault_whenConfiguredActionIsNull() {
        when(dataStreamDetector.isDataStream("regular-index")).thenReturn(false);
        
        String result = dataStreamIndex.determineAction(null, "regular-index");
        
        assertThat(result, equalTo("index"));
    }
    
    @Test
    void ensureTimestamp_setsTimestamp_whenDataStreamAndTimestampMissing() {
        when(dataStreamDetector.isDataStream("my-data-stream")).thenReturn(true);
        when(event.containsKey("@timestamp")).thenReturn(false);
        when(event.getEventHandle()).thenReturn(eventHandle);
        final Instant testTime = Instant.parse("2023-01-01T00:00:00Z");
        when(eventHandle.getInternalOriginationTime()).thenReturn(testTime);
        
        dataStreamIndex.ensureTimestamp(event, "my-data-stream");
        
        verify(event).put("@timestamp", testTime);
    }
    
    @Test
    void ensureTimestamp_doesNotSetTimestamp_whenDataStreamAndTimestampExists() {
        when(dataStreamDetector.isDataStream("my-data-stream")).thenReturn(true);
        when(event.containsKey("@timestamp")).thenReturn(true);
        
        dataStreamIndex.ensureTimestamp(event, "my-data-stream");
        
        verify(event, never()).put("@timestamp", eventHandle.getInternalOriginationTime());
    }
    
    @Test
    void ensureTimestamp_doesNotSetTimestamp_whenNotDataStream() {
        when(dataStreamDetector.isDataStream("regular-index")).thenReturn(false);
        
        dataStreamIndex.ensureTimestamp(event, "regular-index");
        
        verify(event, never()).containsKey("@timestamp");
        verify(event, never()).put("@timestamp", eventHandle.getInternalOriginationTime());
    }
    

}