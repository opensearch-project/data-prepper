package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.LeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * <p>A LeaderPartition is for some tasks that should be done in a single node only. </p>
 * <p>Hence whatever node owns the lease of this partition will be acted as a 'leader'. </p>
 * <p>In this saas source design, a leader node will be responsible for:</p>
 * <ul>
 * <li>Initialization process</li>
 * <li>Crawl the source iteratively and create work for other worker nodes</li>
 * </ul>
 */

public class LeaderPartition extends EnhancedSourcePartition<LeaderProgressState> {

    public static final String PARTITION_TYPE = "LEADER";
    public static final String DEFAULT_PARTITION_KEY = "GLOBAL";
    private static final Logger LOG = LoggerFactory.getLogger(LeaderPartition.class);
    private static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory())
            .registerModule(new JavaTimeModule());
    private final LeaderProgressState state;

    public LeaderPartition() {
        this.state = new LeaderProgressState(Instant.ofEpochMilli(0));
    }

    public LeaderPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.state = convertToPartitionState(sourcePartitionStoreItem.getPartitionProgressState());
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

    public void setLeaderProgressState(LeaderProgressState state) {
        this.state.setLastPollTime(state.getLastPollTime());
    }

    /**
     * Helper method to convert progress state.
     * This is because the state is currently stored as a String in the coordination store.
     *
     * @param serializedPartitionProgressState serialized value of the partition progress state
     * @return returns the converted value of the progress state
     */
    public LeaderProgressState convertToPartitionState(final String serializedPartitionProgressState) {
        if (Objects.isNull(serializedPartitionProgressState)) {
            return null;
        }
        try {
            return objectMapper.readValue(serializedPartitionProgressState, LeaderProgressState.class);
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert string to partition progress state class ", e);
            return null;
        }
    }
}