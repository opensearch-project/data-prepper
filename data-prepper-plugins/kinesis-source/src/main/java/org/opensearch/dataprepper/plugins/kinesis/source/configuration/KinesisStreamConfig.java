/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import software.amazon.awssdk.arns.Arn;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.time.Duration;
import java.util.Objects;

@Getter
public class KinesisStreamConfig {
    // Checkpointing interval
    private static final Duration MINIMAL_CHECKPOINT_INTERVAL = Duration.ofMillis(2 * 60 * 1000); // 2 minute
    private static final boolean DEFAULT_ENABLE_CHECKPOINT = false;

    @JsonProperty("stream_name")
    @Valid
    private String name;

    @JsonProperty("stream_arn")
    @Valid
    private String streamArn;

    @JsonProperty("consumer_arn")
    @Valid
    private String consumerArn;

    @JsonProperty("initial_position")
    private InitialPositionInStreamConfig initialPosition = InitialPositionInStreamConfig.LATEST;

    @JsonProperty("checkpoint_interval")
    private Duration checkPointInterval = MINIMAL_CHECKPOINT_INTERVAL;

    public InitialPositionInStream getInitialPosition() {
        return initialPosition.getPositionInStream();
    }

    @Getter
    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    public String getStreamArn() {
        if (Objects.nonNull(this.streamArn) && !this.streamArn.isEmpty()) {
            try {
                Arn.fromString(this.streamArn);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for stream arn");
            }
        }
        return this.streamArn;
    }

    public String getConsumerArn() {
        if (Objects.nonNull(this.consumerArn) && !this.consumerArn.isEmpty()) {
            try {
                Arn.fromString(this.consumerArn);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for consumer arn");
            }
        }
        return this.consumerArn;
    }

}
