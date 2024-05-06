package org.opensearch.dataprepper.plugins.mongo.converter;

import com.mongodb.client.model.changestream.OperationType;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class PartitionKeyRecordConverter extends RecordConverter {
    public static final String S3_PATH_DELIMITER = "/";
    private List<String> partitionNames = new ArrayList<>();
    private int partitionSize = 0;
    final String s3PathPrefix;
    public PartitionKeyRecordConverter(final String collection, final String partitionType, final String s3PathPrefix) {
        super(collection, partitionType);
        this.s3PathPrefix = s3PathPrefix;
    }

    public void initializePartitions(final List<String> partitionNames) {
        this.partitionNames = partitionNames;
        this.partitionSize = partitionNames.size();
    }

    @Override
    public Event convert(final String record,
                         final long eventCreationTimeMillis,
                         final long eventVersionNumber,
                         final OperationType eventName,
                         final String primaryKeyBsonType) {
        final Event event =  super.convert(record, eventCreationTimeMillis, eventVersionNumber, eventName, primaryKeyBsonType);
        final EventMetadata eventMetadata = event.getMetadata();
        final String partitionKey = String.valueOf(eventMetadata.getAttribute(MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE));
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY, s3PathPrefix + S3_PATH_DELIMITER + hashKeyToPartition(partitionKey));
        return event;
    }

    private String hashKeyToPartition(final String key) {
        return partitionNames.get(hashKeyToIndex(key));
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
            return Math.abs(hashValue) % partitionSize;
        } catch (final NoSuchAlgorithmException
                e) {
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