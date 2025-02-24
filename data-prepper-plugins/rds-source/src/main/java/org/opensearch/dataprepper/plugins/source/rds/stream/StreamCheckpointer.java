/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class StreamCheckpointer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamCheckpointer.class);

    static final Duration CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE = Duration.ofMinutes(5);
    static final String CHECKPOINT_COUNT = "checkpoints";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final StreamPartition streamPartition;
    private final PluginMetrics pluginMetrics;
    private final Counter checkpointCounter;

    public StreamCheckpointer(final EnhancedSourceCoordinator sourceCoordinator,
                              final StreamPartition streamPartition,
                              final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.streamPartition = streamPartition;
        this.pluginMetrics = pluginMetrics;
        checkpointCounter = pluginMetrics.counter(CHECKPOINT_COUNT);
    }

    public void checkpoint(final EngineType engineType, final ChangeEventStatus changeEventStatus) {
        if (engineType.isMySql()) {
            checkpoint(changeEventStatus.getBinlogCoordinate());
        } else if (engineType.isPostgres()) {
            checkpoint(changeEventStatus.getLogSequenceNumber());
        } else {
            throw new IllegalArgumentException("Unsupported engine type " + engineType);
        }
    }

    private void checkpoint(final BinlogCoordinate binlogCoordinate) {
        LOG.debug("Checkpointing stream partition {} with binlog coordinate {}", streamPartition.getPartitionKey(), binlogCoordinate);
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        progressState.get().getMySqlStreamState().setCurrentPosition(binlogCoordinate);
        sourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        checkpointCounter.increment();
    }

    private void checkpoint(final LogSequenceNumber logSequenceNumber) {
        LOG.debug("Checkpointing stream partition {} with log sequence number {}", streamPartition.getPartitionKey(), logSequenceNumber);
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        progressState.get().getPostgresStreamState().setCurrentLsn(logSequenceNumber.asString());
        sourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        checkpointCounter.increment();
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
