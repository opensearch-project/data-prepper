/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.source.rds.model.StreamEventType;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_DATABASE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_S3_PARTITION_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_SCHEMA_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter.S3_BUFFER_PREFIX;
import static org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter.STREAM_INGESTION_TYPE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter.S3_PATH_DELIMITER;


class StreamRecordConverterTest {

    private StreamRecordConverter streamRecordConverter;
    private Random random;
    private String s3Prefix;

    @BeforeEach
    void setUp() {
        random = new Random();
        s3Prefix = UUID.randomUUID().toString();
        streamRecordConverter = createObjectUnderTest();
    }

    @Test
    void test_convert_returns_expected_event() {
        final Map<String, Object> rowData = Map.of("key1", "value1", "key2", "value2");
        final String databaseName = UUID.randomUUID().toString();
        final String schemaName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final StreamEventType eventType = StreamEventType.INSERT;
        final OpenSearchBulkActions bulkAction = OpenSearchBulkActions.INDEX;
        final List<String> primaryKeys = List.of("key1");
        final long eventCreateTimeEpochMillis = random.nextLong();
        final long eventVersionNumber = random.nextLong();

        final Event testEvent = TestEventFactory.getTestEventFactory().eventBuilder(EventBuilder.class)
                .withEventType("event")
                .withData(rowData)
                .build();

        Event event = streamRecordConverter.convert(
                testEvent, databaseName, schemaName, tableName, bulkAction, primaryKeys,
                eventCreateTimeEpochMillis, eventVersionNumber, eventType);

        assertThat(event.toMap(), is(rowData));
        assertThat(event.getMetadata().getAttribute(EVENT_DATABASE_NAME_METADATA_ATTRIBUTE), is(databaseName));
        assertThat(event.getMetadata().getAttribute(EVENT_SCHEMA_NAME_METADATA_ATTRIBUTE), equalTo(schemaName));
        assertThat(event.getMetadata().getAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE), is(tableName));
        assertThat(event.getMetadata().getAttribute(CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE), is(eventType.toString()));
        assertThat(event.getMetadata().getAttribute(BULK_ACTION_METADATA_ATTRIBUTE), is(bulkAction.toString()));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), is("value1"));
        assertThat(event.getMetadata().getAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE), equalTo(STREAM_INGESTION_TYPE));
        assertThat(event.getMetadata().getAttribute(EVENT_S3_PARTITION_KEY).toString(), startsWith(s3Prefix + S3_PATH_DELIMITER + S3_BUFFER_PREFIX + S3_PATH_DELIMITER));
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), is(eventCreateTimeEpochMillis));
        assertThat(event.getMetadata().getAttribute(EVENT_VERSION_FROM_TIMESTAMP), is(eventVersionNumber));
        assertThat(event, sameInstance(testEvent));
    }

    @Test
    void test_getVersionNumber_monotonically_increasing_same_millis() {
        long millis = 1776300000000L;
        long v1 = streamRecordConverter.getVersionNumber(millis);
        long v2 = streamRecordConverter.getVersionNumber(millis);
        long v3 = streamRecordConverter.getVersionNumber(millis);

        assertThat(v1, equalTo(millis * 1000));
        assertThat(v2, equalTo(v1 + 1));
        assertThat(v3, equalTo(v2 + 1));
    }

    @Test
    void test_getVersionNumber_resets_on_new_millis() {
        long millis1 = 1776300000000L;
        long millis2 = 1776300000001L;

        long v1 = streamRecordConverter.getVersionNumber(millis1);
        long v2 = streamRecordConverter.getVersionNumber(millis1);
        long v3 = streamRecordConverter.getVersionNumber(millis2);

        assertThat(v1, equalTo(millis1 * 1000));
        assertThat(v2, equalTo(v1 + 1));
        assertThat(v3, equalTo(millis2 * 1000));
        assertThat(v3 > v2, is(true));
    }

    @Test
    void test_getVersionNumber_always_greater_than_export_version() {
        // Export versions use raw snapshotTimeMillis, stream versions use millis * 1000
        long exportVersion = 1776300000000L;
        long streamVersion = streamRecordConverter.getVersionNumber(exportVersion);

        assertThat(streamVersion, equalTo(exportVersion * 1000));
        assertThat(streamVersion > exportVersion, is(true));
    }

    private StreamRecordConverter createObjectUnderTest() {
        return new StreamRecordConverter(s3Prefix, random.nextInt(1000) + 1);
    }
}