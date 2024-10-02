package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;
import java.util.List;

@Setter
@Getter
public class LeaderProgressState implements SourcePartitionStoreItem {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("last_poll_time")
    private Long lastPollTime;

    public LeaderProgressState(@JsonProperty("last_poll_time") final Long lastPollTime) {
        this.lastPollTime = lastPollTime;
    }

    @Override
    public String getSourceIdentifier() {
        return "";
    }

    @Override
    public String getSourcePartitionKey() {
        return "";
    }

    @Override
    public String getPartitionOwner() {
        return "";
    }

    @Override
    public String getPartitionProgressState() {
        return lastPollTime.toString();
    }

    @Override
    public SourcePartitionStatus getSourcePartitionStatus() {
        return null;
    }

    @Override
    public Instant getPartitionOwnershipTimeout() {
        return null;
    }

    @Override
    public Instant getReOpenAt() {
        return null;
    }

    @Override
    public Long getClosedCount() {
        return 0L;
    }

    @Override
    public void setSourcePartitionKey(String sourcePartitionKey) {

    }

    @Override
    public void setPartitionOwner(String partitionOwner) {

    }

    @Override
    public void setPartitionProgressState(String partitionProgressState) {

    }

    @Override
    public void setSourcePartitionStatus(SourcePartitionStatus sourcePartitionStatus) {

    }

    @Override
    public void setPartitionOwnershipTimeout(Instant partitionOwnershipTimeout) {

    }

    @Override
    public void setReOpenAt(Instant reOpenAt) {

    }

    @Override
    public void setClosedCount(Long closedCount) {

    }
}
