/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import com.github.shyiko.mysql.binlog.event.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_DATABASE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_S3_PARTITION_KEY;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter.S3_PATH_DELIMITER;


class StreamRecordConverterTest {

    private StreamRecordConverter streamRecordConverter;
    private Random random;

    @BeforeEach
    void setUp() {
        streamRecordConverter = createObjectUnderTest();
        random = new Random();
    }

    @Test
    void test_convert_returns_expected_event() {
        final Map<String, Object> rowData = Map.of("key1", "value1", "key2", "value2");
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final EventType eventType = EventType.EXT_WRITE_ROWS;
        final OpenSearchBulkActions bulkAction = OpenSearchBulkActions.INDEX;
        final List<String> primaryKeys = List.of("key1");
        final String s3Prefix = UUID.randomUUID().toString();
        final long eventCreateTimeEpochMillis = random.nextLong();
        final long eventVersionNumber = random.nextLong();

        Event event = streamRecordConverter.convert(
                rowData, databaseName, tableName, eventType, bulkAction,
                primaryKeys, s3Prefix, eventCreateTimeEpochMillis, eventVersionNumber);

        assertThat(event.toMap(), is(rowData));
        assertThat(event.getMetadata().getAttribute(EVENT_DATABASE_NAME_METADATA_ATTRIBUTE), is(databaseName));
        assertThat(event.getMetadata().getAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE), is(tableName));
        assertThat(event.getMetadata().getAttribute(CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE), is(eventType.toString()));
        assertThat(event.getMetadata().getAttribute(BULK_ACTION_METADATA_ATTRIBUTE), is(bulkAction.toString()));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), is("value1"));
        assertThat(event.getMetadata().getAttribute(EVENT_S3_PARTITION_KEY).toString(), startsWith(s3Prefix + S3_PATH_DELIMITER));
        assertThat(event.getMetadata().getAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE), is(eventCreateTimeEpochMillis));
        assertThat(event.getMetadata().getAttribute(EVENT_VERSION_FROM_TIMESTAMP), is(eventVersionNumber));
    }

    private StreamRecordConverter createObjectUnderTest() {
        return new StreamRecordConverter(new Random().nextInt(1000) + 1);
    }
}