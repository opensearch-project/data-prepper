/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.parser.model.SourceCoordinationConfig;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class LeaseBasedSourceCoordinator<T> implements SourceCoordinator<T> {

    private final SourceCoordinationConfig sourceCoordinationConfig;
    private final SourceCoordinationStore sourceCoordinationStore;

    private final Class<T> partitionProgressStateClass;

    public LeaseBasedSourceCoordinator(final Class<T> partitionProgressStateClass,
                                       final SourceCoordinationStore sourceCoordinationStore,
                                       final SourceCoordinationConfig sourceCoordinationConfig) {
        this.sourceCoordinationConfig = sourceCoordinationConfig;
        this.sourceCoordinationStore = sourceCoordinationStore;
        this.partitionProgressStateClass = partitionProgressStateClass;
    }

    @Override
    public void createPartitions(final List<PartitionIdentifier> partitionIdentifiers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SourcePartition<T>> getNextPartition() {
        return Optional.empty();
    }

    @Override
    public void completePartition(final PartitionIdentifier partitionIdentifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closePartition(final PartitionIdentifier partitionIdentifier, final Duration reopenAfter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S extends T> void saveProgressStateForPartition(final PartitionIdentifier partitionIdentifier, final S partitionProgressState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void giveUpPartitions() {
        throw new UnsupportedOperationException();
    }
}
