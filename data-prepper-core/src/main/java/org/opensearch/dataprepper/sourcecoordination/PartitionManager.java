/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;

import java.util.Objects;
import java.util.Optional;

public class PartitionManager<T> {

    private SourcePartition<T> activePartition;

    public PartitionManager() {
    }

    public Optional<SourcePartition<T>> getActivePartition() {
        return Objects.isNull(activePartition) ? Optional.empty() : Optional.of(activePartition);
    }

    public void removeActivePartition() {
        this.activePartition = null;
    }

    public void setActivePartition(final SourcePartition<T> sourcePartition) {
        this.activePartition = sourcePartition;
    }
}
