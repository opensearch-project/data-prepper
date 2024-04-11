package org.opensearch.dataprepper.plugins.mongo.converter;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class PartitionKeyRecordConverter extends RecordConverter {
    private List<String> partitionNames = new ArrayList<>();
    private int partitionSize = 0;
    public PartitionKeyRecordConverter(final String collection, final String partitionType) {
        super(collection, partitionType);
    }

    public void initializePartitions(final List<String> partitionNames) {
        this.partitionNames = partitionNames;
        this.partitionSize = partitionNames.size();
    }

    @Override
    public Event convert(final String record,
                         final long eventCreationTimeMillis,
                         final long eventVersionNumber,
                         final String eventName) {
        final Event event =  super.convert(record, eventCreationTimeMillis, eventVersionNumber, eventName);
        final EventMetadata eventMetadata = event.getMetadata();
        final String partitionKey = String.valueOf(eventMetadata.getAttribute(MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE));
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY, hashKeyToPartition(partitionKey));
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
            //LOG.error("Exception hashing key to index.", e);
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