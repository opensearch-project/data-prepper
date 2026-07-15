/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMin;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.time.Duration;

public class StreamConfig {

    /**
     * Default interval at which the leader node runs shard discovery. DynamoDB Streams shards
     * roll over continuously, so this interval bounds how quickly newly opened child shards are
     * discovered and leased. Lowering it reduces shard-rotation read latency at the cost of more
     * frequent DescribeStream/ListShards calls against DynamoDB Streams.
     */
    static final Duration DEFAULT_SHARD_DISCOVERY_INTERVAL = Duration.ofMinutes(1);

    @JsonProperty(value = "start_position")
    private StreamStartPosition startPosition = StreamStartPosition.LATEST;

    @JsonProperty("view_on_remove")
    private StreamViewType viewForRemoves = StreamViewType.NEW_IMAGE;

    @JsonProperty("disable_checkpointing")
    private boolean disableCheckpointing = false;

    @JsonProperty("shard_discovery_interval")
    @NotNull
    @DurationMin(seconds = 1, message = "shard_discovery_interval must be at least 1 second.")
    private Duration shardDiscoveryInterval = DEFAULT_SHARD_DISCOVERY_INTERVAL;

    public StreamStartPosition getStartPosition() {
        return startPosition;
    }

    public StreamViewType getStreamViewForRemoves() {
        return viewForRemoves;
    }

    public boolean isDisableCheckpointing() { return disableCheckpointing; }

    public Duration getShardDiscoveryInterval() {
        return shardDiscoveryInterval;
    }

}
