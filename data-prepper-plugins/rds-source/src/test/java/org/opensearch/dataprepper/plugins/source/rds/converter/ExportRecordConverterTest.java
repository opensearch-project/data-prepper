/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter.EXPORT_EVENT_TYPE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

@ExtendWith(MockitoExtension.class)
class ExportRecordConverterTest {

    @Test
    void test_convert() {
        final String tableName = UUID.randomUUID().toString();
        final String primaryKeyName = UUID.randomUUID().toString();
        final String primaryKeyValue = UUID.randomUUID().toString();
        final Event testEvent = JacksonEvent.builder()
                .withEventType("EVENT")
                .withData(Map.of(primaryKeyName, primaryKeyValue))
                .build();

        Record<Event> testRecord = new Record<>(testEvent);

        ExportRecordConverter exportRecordConverter = new ExportRecordConverter();
        Event actualEvent = exportRecordConverter.convert(testRecord, tableName, primaryKeyName);

        // Assert
        assertThat(actualEvent.getMetadata().getEventType(), equalTo(EXPORT_EVENT_TYPE));
        assertThat(actualEvent.getMetadata().getAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE), equalTo(tableName));
        assertThat(actualEvent.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(primaryKeyValue));
        assertThat(actualEvent.getMetadata().getAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE), equalTo(EXPORT_EVENT_TYPE));
    }
}