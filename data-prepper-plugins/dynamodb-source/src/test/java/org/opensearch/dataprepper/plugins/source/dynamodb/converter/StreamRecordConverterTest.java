/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import io.micrometer.core.instrument.Counter;
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
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.SORT_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter.CHANGE_EVENTS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter.CHANGE_EVENTS_PROCESSING_ERROR_COUNT;

@ExtendWith(MockitoExtension.class)
class StreamRecordConverterTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    private TableInfo tableInfo;

    @Mock
    private Counter changeEventSuccessCounter;

    @Mock
    private Counter changeEventErrorCounter;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";


    @BeforeEach
    void setup() {

        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName(partitionKeyAttrName)
                .sortKeyAttributeName(sortKeyAttrName)
                .build();

        tableInfo = new TableInfo(tableArn, metadata);

        given(pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT)).willReturn(changeEventSuccessCounter);
        given(pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT)).willReturn(changeEventErrorCounter);

    }


    @Test
    void test_writeToBuffer() throws Exception {

        final Random random = new Random();

        int numberOfRecords = random.nextInt(10);

        List<software.amazon.awssdk.services.dynamodb.model.Record> records = buildRecords(numberOfRecords);

        StreamRecordConverter recordConverter = new StreamRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);
        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();

        recordConverter.writeToBuffer(records);
        verify(bufferAccumulator, times(numberOfRecords)).add(any(Record.class));
        verify(bufferAccumulator).flush();
        verify(changeEventSuccessCounter).increment(anyDouble());

        verifyNoInteractions(changeEventErrorCounter);

    }

    @Test
    void test_writeSingleRecordToBuffer() throws Exception {

        List<software.amazon.awssdk.services.dynamodb.model.Record> records = buildRecords(1);
        final ArgumentCaptor<Record> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        software.amazon.awssdk.services.dynamodb.model.Record record = records.get(0);
        StreamRecordConverter recordConverter = new StreamRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);
        doNothing().when(bufferAccumulator).add(recordArgumentCaptor.capture());
        doNothing().when(bufferAccumulator).flush();

        recordConverter.writeToBuffer(records);

        verify(bufferAccumulator).add(any(Record.class));
        verify(bufferAccumulator).flush();
        verify(changeEventSuccessCounter).increment(anyDouble());
        assertThat(recordArgumentCaptor.getValue().getData(), notNullValue());
        JacksonEvent event = (JacksonEvent) recordArgumentCaptor.getValue().getData();

        assertThat(event.getMetadata(), notNullValue());
        String partitionKey = record.dynamodb().keys().get(partitionKeyAttrName).s();
        String sortKey = record.dynamodb().keys().get(sortKeyAttrName).s();
        assertThat(event.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(partitionKey));
        assertThat(event.getMetadata().getAttribute(SORT_KEY_METADATA_ATTRIBUTE), equalTo(sortKey));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(partitionKey + "_" + sortKey));
        assertThat(event.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.CREATE.toString()));
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(record.dynamodb().approximateCreationDateTime().toEpochMilli()));

        verifyNoInteractions(changeEventErrorCounter);
    }

    private List<software.amazon.awssdk.services.dynamodb.model.Record> buildRecords(int count) {
        List<software.amazon.awssdk.services.dynamodb.model.Record> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, AttributeValue> data = Map.of(
                    partitionKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                    sortKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build());

            StreamRecord streamRecord = StreamRecord.builder()
                    .newImage(data)
                    .keys(data)
                    .sequenceNumber(UUID.randomUUID().toString())
                    .approximateCreationDateTime(Instant.now())
                    .build();
            software.amazon.awssdk.services.dynamodb.model.Record record = software.amazon.awssdk.services.dynamodb.model.Record.builder()
                    .dynamodb(streamRecord)
                    .eventName(OperationType.INSERT)
                    .build();
            records.add(record);
        }

        return records;
    }

}