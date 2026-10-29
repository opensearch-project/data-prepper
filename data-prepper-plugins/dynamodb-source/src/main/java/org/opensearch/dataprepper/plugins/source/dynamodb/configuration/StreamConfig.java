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
    public static final Duration DEFAULT_SHARD_DISCOVERY_INTERVAL = Duration.ofMinutes(1);

    /**
     * Upper bound on how long a worker waits between attempts to acquire a DynamoDB stream shard.
     * A lower value shortens the delay before a new child shard is read after a shard rotation and
     * speeds up how quickly a node claims shards at startup. The cost is more frequent coordination
     * store reads and less even shard distribution across nodes.
     */
    public static final Duration DEFAULT_SHARD_ACQUISITION_BACKOFF_MAX = Duration.ofSeconds(15);

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

    @JsonProperty("shard_acquisition_backoff_max")
    @NotNull
    @DurationMin(seconds = 1, message = "shard_acquisition_backoff_max must be at least 1 second.")
    private Duration shardAcquisitionBackoffMax = DEFAULT_SHARD_ACQUISITION_BACKOFF_MAX;

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

    public Duration getShardAcquisitionBackoffMax() {
        return shardAcquisitionBackoffMax;
    }

}
