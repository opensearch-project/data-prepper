/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;

import java.util.Optional;

/**
 * A helper class to query S3 Folder partition status using source coordinator APIs under the hood.
 */
public class S3FolderPartitionCoordinator {
    private final EnhancedSourceCoordinator enhancedSourceCoordinator;
    private final String collection;


    public S3FolderPartitionCoordinator(final EnhancedSourceCoordinator enhancedSourceCoordinator, final String collection) {
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.collection = collection;
    }

    public Optional<org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus> getGlobalS3FolderCreationStatus() {
        final Optional<EnhancedSourcePartition> partition = enhancedSourceCoordinator.getPartition(S3PartitionCreatorScheduler.S3_FOLDER_PREFIX + collection);
        if(partition.isPresent()) {
            final GlobalState globalState = (GlobalState)partition.get();
            return Optional.of(org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus.fromMap(globalState.getProgressState().get()));
        } else {
            return Optional.empty();
        }
    }
}
