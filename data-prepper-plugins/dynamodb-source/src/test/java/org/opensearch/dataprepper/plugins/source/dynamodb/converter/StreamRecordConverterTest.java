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
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter.CHANGE_EVENTS_PROCESSING_ERROR_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter.CHANGE_EVENTS_PROCESSED_COUNT;

@ExtendWith(MockitoExtension.class)
class StreamRecordConverterTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Buffer<Record<Event>> buffer;

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

        List<software.amazon.awssdk.services.dynamodb.model.Record> data = buildRecords(numberOfRecords);

        StreamRecordConverter recordConverter = new StreamRecordConverter(buffer, tableInfo, pluginMetrics);

        final ArgumentCaptor<Collection<Record<Event>>> writeRequestArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        doNothing().when(buffer).writeAll(writeRequestArgumentCaptor.capture(), anyInt());


        recordConverter.writeToBuffer(data);
        assertThat(writeRequestArgumentCaptor.getValue().size(), equalTo(numberOfRecords));
        verify(changeEventSuccessCounter).increment(anyDouble());

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