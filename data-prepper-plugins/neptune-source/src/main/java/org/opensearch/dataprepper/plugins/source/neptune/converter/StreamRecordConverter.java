package org.opensearch.dataprepper.plugins.source.neptune.converter;

import lombok.Setter;
import lombok.SneakyThrows;
import org.opensearch.dataprepper.model.document.JacksonDocument;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * The record convert transform the source data into a JacksonEvent.
 */
public class StreamRecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRecordConverter.class);
    private static final String S3_PATH_DELIMITER = "/";
    private static final String STREAM_INGESTION_TYPE = "STREAM";
    private static final String STREAM_OP_ADD = "ADD";
    private static final String STREAM_OP_REMOVE = "REMOVE";

    final String s3PathPrefix;
    @Setter
    private List<String> partitions = new ArrayList<>();

    public StreamRecordConverter(final String s3PathPrefix) {
        this.s3PathPrefix = s3PathPrefix;
    }

    /**
     * Convert the source data into a JacksonEvent.
     *
     * @param neptuneRecord record that will be converted to Event
     * @return Jackson document event
     */
    @SneakyThrows
    public Event convert(final NeptuneStreamRecord neptuneRecord) {
        final Event event = JacksonDocument.builder()
                .withData(NeptuneS3Record.fromNeptuneStreamRecord(neptuneRecord))
                .build();
        final EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(MetadataKeyAttributes.ID_METADATA_ATTRIBUTE, neptuneRecord.getId());
        eventMetadata.setAttribute(MetadataKeyAttributes.NEPTUNE_COMMIT_TIMESTAMP_METADATA_ATTRIBUTE, neptuneRecord.getCommitTimestampInMillis());
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP, neptuneRecord.getCommitTimestampInMillis());
        eventMetadata.setAttribute(MetadataKeyAttributes.NEPTUNE_STREAM_OP_NAME_METADATA_ATTRIBUTE, neptuneRecord.getOp());
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, mapStreamEventNameToBulkAction(neptuneRecord.getOp()));
        eventMetadata.setAttribute(MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE, STREAM_INGESTION_TYPE);
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY, this.s3PathPrefix + S3_PATH_DELIMITER + hashKeyToPartition(neptuneRecord.getId()));

        return event;
    }

    private String mapStreamEventNameToBulkAction(final String op) {
        switch (op) {
            // Neptune's data model always uses upsert with custom script to update the document
            case STREAM_OP_REMOVE:
            case STREAM_OP_ADD:
                return OpenSearchBulkActions.UPSERT.toString();
            default:
                throw new RuntimeException("Unknown stream operation: " + op);
        }
    }

    private String hashKeyToPartition(final String key) {
        return partitions.get(hashKeyToIndex(key));
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
            return Math.abs(hashValue) % partitions.size();
        } catch (final NoSuchAlgorithmException e) {
            return -1;
        }
    }

    private static int bytesToInt(byte[] bytes) {
        int result = 0;
        for (int i = 0; i < 4 && i < bytes.length; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }
}
