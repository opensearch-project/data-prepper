/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.buffer;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.EXPORT_RECORDS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.EXPORT_RECORDS_PROCESSING_ERROR_COUNT;

@ExtendWith(MockitoExtension.class)
class ExportRecordBufferWriterTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Counter exportRecordSuccess;

    @Mock
    private Counter exportRecordErrors;

    @Mock
    private Event event;

    final Random random = new Random();


    @BeforeEach
    void setup() {
        given(pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT)).willReturn(exportRecordSuccess);
        given(pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT)).willReturn(exportRecordErrors);

    }

    private List<Event> generateData(int count) {
        final List<Event> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(mock(Event.class));
        }
        return result;
    }

    @Test
    void test_writeToBuffer() throws Exception {
        int numberOfRecords = random.nextInt(10);

        final List<Event> data = generateData(numberOfRecords);
        final ExportRecordBufferWriter recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator,
                pluginMetrics);

        recordBufferWriter.writeToBuffer(null, data);
        verify(bufferAccumulator, times(numberOfRecords)).add(any(Record.class));
        verify(exportRecordSuccess).increment(anyDouble());

        verifyNoInteractions(exportRecordErrors);
    }

    @Test
    void test_writeSingleRecordToBuffer() throws Exception {
        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordBufferWriter recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator,
                pluginMetrics);

        recordBufferWriter.writeToBuffer(null, List.of(event));
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        assertThat(recordArgumentCaptor.getValue().getData(), equalTo(event));

        verify(bufferAccumulator).flush();
        verifyNoInteractions(exportRecordErrors);
    }

    @Test
    void test_writeSingleRecordToBufferWithAcknowledgementSet() throws Exception {
        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordBufferWriter recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator,
                pluginMetrics);
        recordBufferWriter.writeToBuffer(acknowledgementSet, List.of(event));
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(acknowledgementSet).add(event);
        assertThat(recordArgumentCaptor.getValue().getData(), equalTo(event));

        verify(bufferAccumulator).flush();
        verifyNoInteractions(exportRecordErrors);
    }

    @Test
    void test_writeSingleRecordFlushException() throws Exception {
        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordBufferWriter recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator,
                pluginMetrics);
        doThrow(RuntimeException.class).when(bufferAccumulator).flush();

        recordBufferWriter.writeToBuffer(acknowledgementSet, List.of(event));
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(acknowledgementSet).add(event);
        assertThat(recordArgumentCaptor.getValue().getData(), equalTo(event));

        verify(bufferAccumulator).flush();
        verify(exportRecordErrors).increment(1);
    }
}