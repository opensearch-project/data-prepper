/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.model.source.SourcePartition;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationConfig;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinator} when DynamoDB is used at the distributed store
 * for source_coordination
 * @since 2.2
 */
public class DynamoDbSourceCoordinator implements SourceCoordinator {

    private final SourceCoordinationConfig sourceCoordinationConfig;

    public DynamoDbSourceCoordinator(final SourceCoordinationConfig sourceCoordinationConfig) {
        this.sourceCoordinationConfig = sourceCoordinationConfig;
    }

    @Override
    public void createPartitions(List<String> partitionKeys) {

    }

    @Override
    public Optional<SourcePartition> getNextPartition() {
        return Optional.empty();
    }

    @Override
    public void completePartition(String partitionKey) {

    }

    @Override
    public void closePartition(String partitionKey, Duration reopenAfter) {

    }

    @Override
    public void saveStateForPartition(String partitionKey, Object partitionState) {

    }

    @Override
    public void giveUpPartitions() {

    }
}
