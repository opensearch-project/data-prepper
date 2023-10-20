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

import java.util.ArrayList;
import java.util.List;
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
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter.EXPORT_RECORDS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter.EXPORT_RECORDS_PROCESSING_ERROR_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.SORT_KEY_METADATA_ATTRIBUTE;

@ExtendWith(MockitoExtension.class)
class ExportRecordConverterTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    private TableInfo tableInfo;

    @Mock
    private Counter exportRecordSuccess;

    @Mock
    private Counter exportRecordErrors;


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

        given(pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT)).willReturn(exportRecordSuccess);
        given(pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT)).willReturn(exportRecordErrors);

    }

    private List<String> generateData(int count) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final String pk1 = UUID.randomUUID().toString();
            final String sk1 = UUID.randomUUID().toString();

            result.add(" $ion_1_0 {Item:{PK:\"" + pk1 + "\",SK:\"" + sk1 + "\"}}");
        }
        return result;
    }

    @Test
    void test_writeToBuffer() throws Exception {

        final Random random = new Random();

        int numberOfRecords = random.nextInt(10);

        List<String> data = generateData(numberOfRecords);
        ExportRecordConverter recordConverter = new ExportRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);

        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();
        recordConverter.writeToBuffer(data);

        verify(bufferAccumulator, times(numberOfRecords)).add(any(Record.class));
        verify(exportRecordSuccess).increment(anyDouble());

        verifyNoInteractions(exportRecordErrors);

    }

    @Test
    void test_writeSingleRecordToBuffer() throws Exception {
        final String pk = UUID.randomUUID().toString();
        final String sk = UUID.randomUUID().toString();
        String line = " $ion_1_0 {Item:{PK:\"" + pk + "\",SK:\"" + sk + "\"}}";

        final ArgumentCaptor<Record> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        ExportRecordConverter recordConverter = new ExportRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);
        doNothing().when(bufferAccumulator).add(recordArgumentCaptor.capture());
        doNothing().when(bufferAccumulator).flush();

        recordConverter.writeToBuffer(List.of(line));

        verify(bufferAccumulator).add(any(Record.class));
        verify(bufferAccumulator).flush();
        assertThat(recordArgumentCaptor.getValue().getData(), notNullValue());
        JacksonEvent event = (JacksonEvent) recordArgumentCaptor.getValue().getData();

        assertThat(event.getMetadata(), notNullValue());

        assertThat(event.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(pk));
        assertThat(event.getMetadata().getAttribute(SORT_KEY_METADATA_ATTRIBUTE), equalTo(sk));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(pk + "_" + sk));
        assertThat(event.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), notNullValue());
    }
}