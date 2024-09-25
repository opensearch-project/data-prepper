package org.opensearch.dataprepper.connector.sdk.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.ArrayList;
import java.util.Optional;

/**
 * <p>A LeaderPartition is for some tasks that should be done in a single node only. </p>
 * <p>Hence whatever node owns the lease of this partition will be acted as a 'leader'. </p>
 * <p>In this DynamoDB source design, a leader node will be responsible for:</p>
 * <ul>
 * <li>Initialization process</li>
 * <li>Regular Shard Discovery</li>
 * </ul>
 */
public class LeaderPartition extends EnhancedSourcePartition<LeaderProgressState> {

    public static final String PARTITION_TYPE = "LEADER";

    private static final String DEFAULT_PARTITION_KEY = "GLOBAL";

    private final LeaderProgressState state;

    public LeaderPartition() {
        this.state = new LeaderProgressState();
        this.state.setInitialized(false);
        this.state.setStreamArns(new ArrayList<>());
    }

    public LeaderPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.state = convertStringToPartitionProgressState(LeaderProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
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