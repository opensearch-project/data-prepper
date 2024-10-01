/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.Optional;

/**
 * A S3 Folder partition represents an S3 partition job to create S3 path prefix/sub folder that will
 * be used to group records based on record key.
 */
public class S3FolderPartition extends EnhancedSourcePartition<String> {

    public static final String PARTITION_TYPE = "S3_FOLDER";
    private final String bucketName;
    private final String pathPrefix;
    private final String region;
    private final int partitionCount;

    public S3FolderPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        bucketName = keySplits[0];
        pathPrefix = keySplits[1];
        partitionCount = Integer.parseInt(keySplits[2]);
        region = keySplits[3];
    }

    public S3FolderPartition(final String bucketName, final String pathPrefix, final String region, final int partitionCount) {
        this.bucketName = bucketName;
        this.pathPrefix = pathPrefix;
        this.region = region;
        this.partitionCount = partitionCount;
    }
    
    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return bucketName + "|" + pathPrefix + "|" + partitionCount + "|" + region;
    }

    @Override
    public Optional<String> getProgressState() {
        return Optional.empty();
    }


    public String getBucketName() {
        return bucketName;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public String getRegion() {
        return region;
    }

    public int getPartitionCount() {
        return partitionCount;
    }
}
