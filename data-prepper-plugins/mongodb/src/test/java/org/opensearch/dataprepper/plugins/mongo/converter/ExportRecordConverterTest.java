/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.converter;

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
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.mongo.converter.ExportRecordConverter.BYTES_PROCESSED;
import static org.opensearch.dataprepper.plugins.mongo.converter.ExportRecordConverter.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.mongo.converter.ExportRecordConverter.EXPORT_RECORDS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.converter.ExportRecordConverter.EXPORT_RECORDS_PROCESSING_ERROR_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

@ExtendWith(MockitoExtension.class)
class ExportRecordConverterTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    @Mock
    private Counter exportRecordSuccess;

    @Mock
    private Counter exportRecordErrors;

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
        final ExportRecordConverter recordConverter = new ExportRecordConverter(bufferAccumulator, collectionConfig,
                pluginMetrics, Instant.now().toEpochMilli());

        recordConverter.writeToBuffer(null, data);
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

        final ArgumentCaptor<Record> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final ExportRecordConverter recordConverter = new ExportRecordConverter(bufferAccumulator, collectionConfig,
                pluginMetrics, exportStartTime);
        doNothing().when(bufferAccumulator).add(recordArgumentCaptor.capture());

        recordConverter.writeToBuffer(eq(null), List.of(record));
        verify(bufferAccumulator).add(any(Record.class));
        verify(bufferAccumulator).flush();
        assertThat(recordArgumentCaptor.getValue().getData(), notNullValue());
        JacksonEvent event = (JacksonEvent) recordArgumentCaptor.getValue().getData();

        assertThat(event.getMetadata(), notNullValue());

        assertThat(event.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(id));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(id));
        assertThat(event.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), notNullValue());
        assertThat(event.getMetadata().getAttribute(MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), nullValue());
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(exportStartTime));
        assertThat(event.getEventHandle(), notNullValue());
        assertThat(event.getEventHandle().getExternalOriginationTime(), nullValue());
        verify(bytesReceivedSummary, times(1)).record(record.getBytes().length);
        verify(bytesProcessedSummary, times(1)).record(record.getBytes().length);
    }
}