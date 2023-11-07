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
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_DYNAMODB_ITEM_VERSION;
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

        List<software.amazon.awssdk.services.dynamodb.model.Record> records = buildRecords(numberOfRecords, Instant.now());

        StreamRecordConverter recordConverter = new StreamRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);

        recordConverter.writeToBuffer(null, records);
        verify(bufferAccumulator, times(numberOfRecords)).add(any(Record.class));
        verify(bufferAccumulator).flush();
        verify(changeEventSuccessCounter).increment(anyDouble());

        verifyNoInteractions(changeEventErrorCounter);

    }

    @Test
    void test_writeSingleRecordToBuffer() throws Exception {

        List<software.amazon.awssdk.services.dynamodb.model.Record> records = buildRecords(1, Instant.now());
        final ArgumentCaptor<Record> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        software.amazon.awssdk.services.dynamodb.model.Record record = records.get(0);
        StreamRecordConverter recordConverter = new StreamRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);
        doNothing().when(bufferAccumulator).add(recordArgumentCaptor.capture());

        recordConverter.writeToBuffer(null, records);

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
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(partitionKey + "|" + sortKey));
        assertThat(event.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(event.getMetadata().getAttribute(DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), equalTo("INSERT"));
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(record.dynamodb().approximateCreationDateTime().toEpochMilli()));

        verifyNoInteractions(changeEventErrorCounter);
    }

    @Test
    void writingToBuffer_with_nth_event_in_that_second_returns_expected_that_timestamp() throws Exception {
        final long currentSecond = 1699336310;
        final Instant timestamp = Instant.ofEpochSecond(currentSecond);
        final Instant olderSecond = Instant.ofEpochSecond(currentSecond - 1);
        final Instant newerSecond = Instant.ofEpochSecond(currentSecond + 1);

        List<software.amazon.awssdk.services.dynamodb.model.Record> records = buildRecords(2, timestamp);
        records.add(buildRecord(olderSecond));
        records.add(buildRecord(newerSecond));

        final ArgumentCaptor<Record> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        StreamRecordConverter recordConverter = new StreamRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);
        doNothing().when(bufferAccumulator).add(recordArgumentCaptor.capture());

        recordConverter.writeToBuffer(null, records);

        verify(bufferAccumulator, times(4)).add(any(Record.class));
        verify(bufferAccumulator).flush();
        verify(changeEventSuccessCounter).increment(anyDouble());
        assertThat(recordArgumentCaptor.getValue().getData(), notNullValue());

        final List<Record> createdEvents = recordArgumentCaptor.getAllValues();

        assertThat(createdEvents.size(), equalTo(records.size()));

        JacksonEvent firstEventForSecond = (JacksonEvent) createdEvents.get(0).getData();

        assertThat(firstEventForSecond.getMetadata(), notNullValue());
        String partitionKey = records.get(0).dynamodb().keys().get(partitionKeyAttrName).s();
        String sortKey = records.get(0).dynamodb().keys().get(sortKeyAttrName).s();
        assertThat(firstEventForSecond.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(partitionKey));
        assertThat(firstEventForSecond.getMetadata().getAttribute(SORT_KEY_METADATA_ATTRIBUTE), equalTo(sortKey));
        assertThat(firstEventForSecond.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(partitionKey + "|" + sortKey));
        assertThat(firstEventForSecond.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(firstEventForSecond.getMetadata().getAttribute(DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), equalTo("INSERT"));
        assertThat(firstEventForSecond.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(timestamp.toEpochMilli()));
        assertThat(firstEventForSecond.getMetadata().getAttribute(EVENT_DYNAMODB_ITEM_VERSION), equalTo(timestamp.toEpochMilli() * 1000));
        assertThat(firstEventForSecond.getEventHandle(), notNullValue());
        assertThat(firstEventForSecond.getEventHandle().getExternalOriginationTime(), equalTo(timestamp));

        JacksonEvent secondEventForSameSecond = (JacksonEvent) createdEvents.get(1).getData();

        assertThat(secondEventForSameSecond.getMetadata(), notNullValue());
        String secondPartitionKey = records.get(1).dynamodb().keys().get(partitionKeyAttrName).s();
        String secondSortKey = records.get(1).dynamodb().keys().get(sortKeyAttrName).s();
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(secondPartitionKey));
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(SORT_KEY_METADATA_ATTRIBUTE), equalTo(secondSortKey));
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(secondPartitionKey + "|" + secondSortKey));
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), equalTo("INSERT"));
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(timestamp.toEpochMilli()));
        assertThat(secondEventForSameSecond.getMetadata().getAttribute(EVENT_DYNAMODB_ITEM_VERSION), equalTo(timestamp.toEpochMilli() * 1000 + 1));
        assertThat(secondEventForSameSecond.getEventHandle(), notNullValue());
        assertThat(secondEventForSameSecond.getEventHandle().getExternalOriginationTime(), equalTo(timestamp));

        JacksonEvent thirdEventWithOlderSecond = (JacksonEvent) createdEvents.get(2).getData();

        assertThat(thirdEventWithOlderSecond.getMetadata(), notNullValue());
        String thirdPartitionKey = records.get(2).dynamodb().keys().get(partitionKeyAttrName).s();
        String thirdSortKey = records.get(2).dynamodb().keys().get(sortKeyAttrName).s();
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(thirdPartitionKey));
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(SORT_KEY_METADATA_ATTRIBUTE), equalTo(thirdSortKey));
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(thirdPartitionKey + "|" + thirdSortKey));
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), equalTo("INSERT"));
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(olderSecond.toEpochMilli()));
        assertThat(thirdEventWithOlderSecond.getMetadata().getAttribute(EVENT_DYNAMODB_ITEM_VERSION), equalTo(olderSecond.toEpochMilli() * 1000));
        assertThat(thirdEventWithOlderSecond.getEventHandle(), notNullValue());
        assertThat(thirdEventWithOlderSecond.getEventHandle().getExternalOriginationTime(), equalTo(olderSecond));

        JacksonEvent fourthEventWithNewerSecond = (JacksonEvent) createdEvents.get(3).getData();

        assertThat(fourthEventWithNewerSecond.getMetadata(), notNullValue());
        String fourthPartitionKey = records.get(3).dynamodb().keys().get(partitionKeyAttrName).s();
        String fourthSortKey = records.get(3).dynamodb().keys().get(sortKeyAttrName).s();
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(fourthPartitionKey));
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(SORT_KEY_METADATA_ATTRIBUTE), equalTo(fourthSortKey));
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(fourthPartitionKey + "|" + fourthSortKey));
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), equalTo("INSERT"));
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(newerSecond.toEpochMilli()));
        assertThat(fourthEventWithNewerSecond.getMetadata().getAttribute(EVENT_DYNAMODB_ITEM_VERSION), equalTo(newerSecond.toEpochMilli() * 1000));
        assertThat(fourthEventWithNewerSecond.getEventHandle(), notNullValue());
        assertThat(fourthEventWithNewerSecond.getEventHandle().getExternalOriginationTime(), equalTo(newerSecond));

        verifyNoInteractions(changeEventErrorCounter);
    }

    private List<software.amazon.awssdk.services.dynamodb.model.Record> buildRecords(int count, final Instant creationTime) {
        List<software.amazon.awssdk.services.dynamodb.model.Record> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(buildRecord(creationTime));
        }

        return records;
    }

    private software.amazon.awssdk.services.dynamodb.model.Record buildRecord(final Instant creationTime) {
        Map<String, AttributeValue> data = Map.of(
                partitionKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                sortKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build());
        StreamRecord streamRecord = StreamRecord.builder()
                .newImage(data)
                .keys(data)
                .sequenceNumber(UUID.randomUUID().toString())
                .approximateCreationDateTime(creationTime)
                .build();
        software.amazon.awssdk.services.dynamodb.model.Record record = software.amazon.awssdk.services.dynamodb.model.Record.builder()
                .dynamodb(streamRecord)
                .eventName(OperationType.INSERT)
                .build();
        return record;
    }

}