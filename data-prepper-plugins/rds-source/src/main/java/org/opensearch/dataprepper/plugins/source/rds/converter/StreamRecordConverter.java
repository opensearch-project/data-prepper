/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import com.github.shyiko.mysql.binlog.event.EventType;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_DATABASE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

/**
 * Convert binlog row data into JacksonEvent
 */
public class StreamRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(StreamRecordConverter.class);

    private final List<String> folderNames;

    static final String S3_PATH_DELIMITER = "/";

    static final String STREAM_EVENT_TYPE = "STREAM";

    public StreamRecordConverter(final int partitionCount) {
        S3PartitionCreator s3PartitionCreator = new S3PartitionCreator(partitionCount);
        folderNames = s3PartitionCreator.createPartitions();
    }

    public Event convert(Map<String, Object> rowData,
                         String databaseName,
                         String tableName,
                         EventType eventType,
                         OpenSearchBulkActions bulkAction,
                         List<String> primaryKeys,
                         String s3Prefix) {
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(rowData)
                .build();

        EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(EVENT_DATABASE_NAME_METADATA_ATTRIBUTE, databaseName);
        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableName);
        eventMetadata.setAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, bulkAction.toString());
        eventMetadata.setAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE, STREAM_EVENT_TYPE);
        eventMetadata.setAttribute(CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE, eventType.toString());

        final String primaryKeyValue = primaryKeys.stream()
                .map(rowData::get)
                .map(String::valueOf)
                .collect(Collectors.joining("|"));
        eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, primaryKeyValue);
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY, s3Prefix + S3_PATH_DELIMITER + hashKeyToPartition(primaryKeyValue));

        return event;
    }

    private String hashKeyToPartition(final String key) {
        return folderNames.get(hashKeyToIndex(key));
    }
    private int hashKeyToIndex(final String key) {
        try {
            // Create a SHA-256 hash instance
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Hash the key
            byte[] hashBytes = digest.digest(key.getBytes());
            // Convert the hash to an integer
            int hashValue = bytesToInt(hashBytes);
            // Map the hash value to an index in the list
            return Math.abs(hashValue) % folderNames.size();
        } catch (final NoSuchAlgorithmException e) {
            return -1;
        }
    }
    private int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }
}
