/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.source.SourcePartition;

import java.util.Objects;
import java.util.Optional;

public class PartitionManager {

    private SourcePartition activePartition;

    public PartitionManager() {
    }

    public Optional<SourcePartition> getActivePartition() {
        return Objects.isNull(activePartition) ? Optional.empty() : Optional.of(activePartition);
    }

    public void removeActivePartition() {
        this.activePartition = null;
    }

    public void setActivePartition(final SourcePartition sourcePartition) {
        this.activePartition = sourcePartition;
    }
}
