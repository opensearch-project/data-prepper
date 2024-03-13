/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.buffer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
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
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.RecordConverter;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.BYTES_PROCESSED;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.EXPORT_RECORDS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.EXPORT_RECORDS_PROCESSING_ERROR_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter.VERSION_OVERLAP_TIME_FOR_EXPORT;

@ExtendWith(MockitoExtension.class)
class ExportRecordBufferWriterTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private RecordConverter recordConverter;

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

    @Mock
    private DistributionSummary bytesReceivedSummary;

    @Mock
    private DistributionSummary bytesProcessedSummary;


    private CollectionConfig collectionConfig;

    final Random random = new Random();


    @BeforeEach
    void setup() throws Exception {
        collectionConfig = new CollectionConfig();
        ReflectivelySetField.setField(CollectionConfig.class, collectionConfig, "collection", "staging.products");
        given(pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT)).willReturn(exportRecordSuccess);
        given(pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT)).willReturn(exportRecordErrors);
        given(pluginMetrics.summary(BYTES_RECEIVED)).willReturn(bytesReceivedSummary);
        given(pluginMetrics.summary(BYTES_PROCESSED)).willReturn(bytesProcessedSummary);

    }

    private List<String> generateData(int count) {
        final List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add("{" +
                    "\"_id\":\"" +  UUID.randomUUID() + "\"," +
                    "\"customerId\":" + random.nextInt() + "," +
                    "\"productId\":" + random.nextInt() + "," +
                    "\"quantity\":" + random.nextInt() + "," +
                    "\"orderDate\":{\"date\":\"" + LocalDate.now() +"\"}}"
            );
        }
        return result;
    }

    @Test
    void test_writeToBuffer() throws Exception {
        int numberOfRecords = random.nextInt(10);

        final List<String> data = generateData(numberOfRecords);
        final ExportRecordBufferWriter recordBufferWriter = new ExportRecordBufferWriter(bufferAccumulator, collectionConfig,
                recordConverter, pluginMetrics, Instant.now().toEpochMilli());

        recordBufferWriter.writeToBuffer(null, data);
        verify(bufferAccumulator, times(numberOfRecords)).add(any(Record.class));
        verify(exportRecordSuccess).increment(anyDouble());

        verifyNoInteractions(exportRecordErrors);
        verify(bytesReceivedSummary, times(numberOfRecords)).record(anyDouble());
        verify(bytesProcessedSummary, times(numberOfRecords)).record(anyDouble());
    }

    @Test
    void test_writeSingleRecordToBuffer() throws Exception {
        final String id = UUID.randomUUID().toString();
        final String record = "{" +
                "\"_id\":\"" + id + "\"," +
                "\"customerId\":" + random.nextInt() + "," +
                "\"productId\":" + random.nextInt() + "," +
                "\"quantity\":" + random.nextInt() + "," +
                "\"orderDate\":{\"date\":\"" + LocalDate.now() +"\"}}";
        final long exportStartTime = Instant.now().toEpochMilli();
        final long eventVersionNumber = (exportStartTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis()) * 1_000;

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordBufferWriter recordBufferWriter = new ExportRecordBufferWriter(bufferAccumulator, collectionConfig,
                recordConverter, pluginMetrics, exportStartTime);
        when(recordConverter.convert(record, exportStartTime, eventVersionNumber, null)).thenReturn(event);

        recordBufferWriter.writeToBuffer(null, List.of(record));
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        assertThat(recordArgumentCaptor.getValue().getData(), equalTo(event));

        verify(bufferAccumulator).flush();
        verifyNoInteractions(exportRecordErrors);
        verify(bytesReceivedSummary).record(anyDouble());
        verify(bytesProcessedSummary).record(anyDouble());
    }

    @Test
    void test_writeSingleRecordToBufferWithAcknowledgementSet() throws Exception {
        final String id = UUID.randomUUID().toString();
        final String record = "{" +
                "\"_id\":\"" + id + "\"," +
                "\"customerId\":" + random.nextInt() + "," +
                "\"productId\":" + random.nextInt() + "," +
                "\"quantity\":" + random.nextInt() + "," +
                "\"orderDate\":{\"date\":\"" + LocalDate.now() +"\"}}";
        final long exportStartTime = Instant.now().toEpochMilli();
        final long eventVersionNumber = (exportStartTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis()) * 1_000;

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordBufferWriter recordBufferWriter = new ExportRecordBufferWriter(bufferAccumulator, collectionConfig,
                recordConverter, pluginMetrics, exportStartTime);
        when(recordConverter.convert(record, exportStartTime, eventVersionNumber, null)).thenReturn(event);

        recordBufferWriter.writeToBuffer(acknowledgementSet, List.of(record));
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(acknowledgementSet).add(event);
        assertThat(recordArgumentCaptor.getValue().getData(), equalTo(event));

        verify(bufferAccumulator).flush();
        verifyNoInteractions(exportRecordErrors);
        verify(bytesReceivedSummary).record(anyDouble());
        verify(bytesProcessedSummary).record(anyDouble());
    }

    @Test
    void test_writeSingleRecordFlushException() throws Exception {
        final String id = UUID.randomUUID().toString();
        final String record = "{" +
                "\"_id\":\"" + id + "\"," +
                "\"customerId\":" + random.nextInt() + "," +
                "\"productId\":" + random.nextInt() + "," +
                "\"quantity\":" + random.nextInt() + "," +
                "\"orderDate\":{\"date\":\"" + LocalDate.now() +"\"}}";
        final long exportStartTime = Instant.now().toEpochMilli();
        final long eventVersionNumber = (exportStartTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis()) * 1_000;

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordBufferWriter recordBufferWriter = new ExportRecordBufferWriter(bufferAccumulator, collectionConfig,
                recordConverter, pluginMetrics, exportStartTime);
        when(recordConverter.convert(record, exportStartTime, eventVersionNumber, null)).thenReturn(event);
        doThrow(RuntimeException.class).when(bufferAccumulator).flush();

        recordBufferWriter.writeToBuffer(acknowledgementSet, List.of(record));
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        verify(acknowledgementSet).add(event);
        assertThat(recordArgumentCaptor.getValue().getData(), equalTo(event));

        verify(bufferAccumulator).flush();
        verify(exportRecordErrors).increment(1);
        verify(bytesReceivedSummary).record(anyDouble());
        verify(bytesProcessedSummary).record(anyDouble());
    }
}