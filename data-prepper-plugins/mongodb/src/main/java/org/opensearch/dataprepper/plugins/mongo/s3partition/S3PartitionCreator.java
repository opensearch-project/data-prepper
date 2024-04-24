package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class S3PartitionCreator {
    private static final Logger LOG = LoggerFactory.getLogger(S3PartitionCreator.class);
    private final int partitionCount;

    S3PartitionCreator(final int partitionCount) {
        this.partitionCount = partitionCount;
    }

    List<String> createPartition() {
        final List<String> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            String partitionName = String.format("%02x", i) + "/";
            partitions.add(partitionName);
        }
        LOG.info("S3 partition created successfully.");
        return partitions;
    }
}