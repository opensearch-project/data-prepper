/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.source.rds.model.StreamEventType;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_DATABASE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_SCHEMA_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

public abstract class RecordConverter {

    private final String s3Prefix;
    private final List<String> folderNames;

    static final String S3_BUFFER_PREFIX = "buffer";
    static final String S3_PATH_DELIMITER = "/";
    static final String EXPORT_INGESTION_TYPE = "EXPORT";
    static final String STREAM_INGESTION_TYPE = "STREAM";


    public RecordConverter(final String s3Prefix, final int partitionCount) {
        this.s3Prefix = s3Prefix;
        S3PartitionCreator s3PartitionCreator = new S3PartitionCreator(partitionCount);
        folderNames = s3PartitionCreator.createPartitions();
    }

    public Event convert(final Event event,
                         final String databaseName,
                         final String schemaName,
                         final String tableName,
                         final OpenSearchBulkActions bulkAction,
                         final List<String> primaryKeys,
                         final long eventCreateTimeEpochMillis,
                         final long eventVersionNumber,
                         final StreamEventType eventType) {

        EventMetadata eventMetadata = event.getMetadata();

        // Only set external origination time for stream events, not export
        if (STREAM_INGESTION_TYPE.equals(getIngestionType())) {
            final Instant externalOriginationTime = Instant.ofEpochMilli(eventCreateTimeEpochMillis);
            event.getEventHandle().setExternalOriginationTime(externalOriginationTime);
            eventMetadata.setExternalOriginationTime(externalOriginationTime);
        }

        eventMetadata.setAttribute(EVENT_DATABASE_NAME_METADATA_ATTRIBUTE, databaseName);
        eventMetadata.setAttribute(EVENT_SCHEMA_NAME_METADATA_ATTRIBUTE, schemaName);
        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableName);
        eventMetadata.setAttribute(BULK_ACTION_METADATA_ATTRIBUTE, bulkAction.toString());
        setIngestionTypeMetadata(event);
        if (eventType != null) {
            eventMetadata.setAttribute(CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE, eventType.toString());
        }

        final String primaryKeyValue = primaryKeys.stream()
                .map(key -> event.get(key, String.class))
                .collect(Collectors.joining("|"));
        eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, primaryKeyValue);

        final String s3PartitionKey = s3Prefix + S3_PATH_DELIMITER + S3_BUFFER_PREFIX + S3_PATH_DELIMITER + hashKeyToPartition(primaryKeyValue);
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY, s3PartitionKey);

        eventMetadata.setAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreateTimeEpochMillis);
        eventMetadata.setAttribute(EVENT_VERSION_FROM_TIMESTAMP, eventVersionNumber);

        return event;
    }

    abstract String getIngestionType();

    private void setIngestionTypeMetadata(final Event event) {
        event.getMetadata().setAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE, getIngestionType());
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
