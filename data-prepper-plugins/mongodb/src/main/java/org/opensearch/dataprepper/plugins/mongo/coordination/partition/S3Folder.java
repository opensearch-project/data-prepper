/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.Optional;

/**
 * A S3 Folder partition represents S3 prefix path for group of records.
 */
public class S3Folder extends EnhancedSourcePartition<String> {

    public static final String PARTITION_TYPE = "S3_FOLDER";
    private final String bucketName;
    private final String subFolder;
    private final String partitionFolder;
    private final String region;
    private final String collection;

    public S3Folder(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        collection = keySplits[0];
        bucketName = keySplits[1];
        subFolder = keySplits[2];
        partitionFolder = keySplits[3];
        region = keySplits[4];
    }

    public S3Folder(final String collection, final String bucketName, final String subFolder, final String partitionFolder, final String region) {
        this.collection = collection;
        this.bucketName = bucketName;
        this.subFolder = subFolder;
        this.partitionFolder = partitionFolder;
        this.region = region;
    }
    
    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return collection + "|" + bucketName + "|" + subFolder + "|" + partitionFolder + "|" + region;
    }

    @Override
    public Optional<String> getProgressState() {
        /*if (state != null) {
            return Optional.of(state);
        }*/
        return Optional.empty();
    }


    public String getBucketName() {
        return bucketName;
    }

    public String getSubFolder() {
        return subFolder;
    }

    public String getPartitionFolder() {
        return partitionFolder;
    }

    public String getRegion() {
        return region;
    }

    public String getCollection() {
        return collection;
    }

    public String getPartitionFolderPath() {
        if (subFolder != null) {
            return subFolder + '/' + partitionFolder;
        } else {
            return partitionFolder;
        }
    }
}
