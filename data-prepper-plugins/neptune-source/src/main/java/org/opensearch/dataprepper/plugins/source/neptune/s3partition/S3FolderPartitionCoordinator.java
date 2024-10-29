/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.s3partition;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.neptune.model.S3PartitionStatus;

import java.util.Optional;

/**
 * A helper class to query S3 Folder partition status using source coordinator APIs under the hood.
 */
public class S3FolderPartitionCoordinator {
    private final EnhancedSourceCoordinator enhancedSourceCoordinator;

    public S3FolderPartitionCoordinator(final EnhancedSourceCoordinator enhancedSourceCoordinator) {
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
    }

    public Optional<S3PartitionStatus> getGlobalS3FolderCreationStatus() {
        final Optional<EnhancedSourcePartition> partition = enhancedSourceCoordinator.getPartition(S3PartitionCreatorScheduler.S3_FOLDER_PREFIX);
        if (partition.isPresent()) {
            final GlobalState globalState = (GlobalState) partition.get();
            return Optional.of(S3PartitionStatus.fromMap(globalState.getProgressState().get()));
        } else {
            return Optional.empty();
        }
    }
}
