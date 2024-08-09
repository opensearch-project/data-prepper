/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class StreamCheckpointer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamCheckpointer.class);

    static final Duration CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE = Duration.ofMinutes(5);

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final StreamPartition streamPartition;

    public StreamCheckpointer(final EnhancedSourceCoordinator sourceCoordinator, final StreamPartition streamPartition) {
        this.sourceCoordinator = sourceCoordinator;
        this.streamPartition = streamPartition;
    }

    public void checkpoint(final BinlogCoordinate binlogCoordinate) {
        LOG.debug("Checkpointing stream partition {} with binlog coordinate {}", streamPartition.getPartitionKey(), binlogCoordinate);
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        progressState.get().setCurrentPosition(binlogCoordinate);
        sourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    public void extendLease() {
        LOG.debug("Extending lease of stream partition {}", streamPartition.getPartitionKey());
        sourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    public void giveUpPartition() {
        LOG.debug("Giving up stream partition {}", streamPartition.getPartitionKey());
        sourceCoordinator.giveUpPartition(streamPartition);
    }
}
