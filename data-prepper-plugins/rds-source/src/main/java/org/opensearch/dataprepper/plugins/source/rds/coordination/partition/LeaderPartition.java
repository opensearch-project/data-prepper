/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.LeaderProgressState;

import java.util.Optional;

/**
 * <p>A LeaderPartition is for some tasks that should be done in a single node only. </p>
 * <p>Hence whatever node owns the lease of this partition will be acted as a 'leader'. </p>
 * <p>In this DynamoDB source design, a leader node will be responsible for:</p>
 * <ul>
 * <li>Initialization process (create EXPORT and STREAM partitions)</li>
 * <li>Triggering RDS export task</li>
 * <li>Reading stream data</li>
 * </ul>
 */
public class LeaderPartition extends EnhancedSourcePartition<LeaderProgressState> {
    public static final String PARTITION_TYPE = "LEADER";

    // identifier for the partition
    private static final String DEFAULT_PARTITION_KEY = "GLOBAL";

    private final LeaderProgressState state;

    public LeaderPartition() {
        this.state = new LeaderProgressState();
    }

    public LeaderPartition(SourcePartitionStoreItem partitionStoreItem) {
        setSourcePartitionStoreItem(partitionStoreItem);
        this.state = convertStringToPartitionProgressState(LeaderProgressState.class, partitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return DEFAULT_PARTITION_KEY;
    }

    @Override
    public Optional<LeaderProgressState> getProgressState() {
        return Optional.of(state);
    }
}
