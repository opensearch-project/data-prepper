/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter.EXPORT_INGESTION_TYPE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_DATABASE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_S3_PARTITION_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter.S3_BUFFER_PREFIX;
import static org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter.S3_PATH_DELIMITER;

@ExtendWith(MockitoExtension.class)
class ExportRecordConverterTest {

    private Random random;
    private String s3Prefix;
    private ExportRecordConverter exportRecordConverter;

    @BeforeEach
    void setUp() {
        random = new Random();
        s3Prefix = UUID.randomUUID().toString();
        exportRecordConverter = createObjectUnderTest();
    }

    @Test
    void test_convert() {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final String primaryKeyName = UUID.randomUUID().toString();
        final List<String> primaryKeys = List.of(primaryKeyName);
        final String primaryKeyValue = UUID.randomUUID().toString();
        final long eventCreateTimeEpochMillis = random.nextLong();
        final long eventVersionNumber = random.nextLong();

        final Event testEvent = TestEventFactory.getTestEventFactory().eventBuilder(EventBuilder.class)
                .withEventType("event")
                .withData(Map.of(primaryKeyName, primaryKeyValue))
                .build();

        Event actualEvent = exportRecordConverter.convert(
                testEvent, databaseName, tableName, OpenSearchBulkActions.INDEX, primaryKeys,
                eventCreateTimeEpochMillis, eventVersionNumber, null);

        // Assert
        assertThat(actualEvent.getMetadata().getAttribute(EVENT_DATABASE_NAME_METADATA_ATTRIBUTE), equalTo(databaseName));
        assertThat(actualEvent.getMetadata().getAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE), equalTo(tableName));
        assertThat(actualEvent.getMetadata().getAttribute(BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(actualEvent.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(primaryKeyValue));
        assertThat(actualEvent.getMetadata().getAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE), equalTo(EXPORT_INGESTION_TYPE));
        assertThat(actualEvent.getMetadata().getAttribute(EVENT_S3_PARTITION_KEY).toString(), startsWith(s3Prefix + S3_PATH_DELIMITER + S3_BUFFER_PREFIX + S3_PATH_DELIMITER));
        assertThat(actualEvent.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(eventCreateTimeEpochMillis));
        assertThat(actualEvent.getMetadata().getAttribute(EVENT_VERSION_FROM_TIMESTAMP), equalTo(eventVersionNumber));
        assertThat(actualEvent.getMetadata().getAttribute(CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE), nullValue());
        assertThat(actualEvent, sameInstance(testEvent));
    }

    private ExportRecordConverter createObjectUnderTest() {
        return new ExportRecordConverter(s3Prefix, random.nextInt(1000) + 1);
    }
}