package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import software.amazon.kinesis.common.InitialPositionInStream;

@Getter
public class KinesisStreamConfig {
    // Checkpointing interval
    private static final int MINIMAL_CHECKPOINT_INTERVAL_MILLIS = 2 * 60 * 1000; // 2 minute
    private static final boolean DEFAULT_ENABLE_CHECKPOINT = false;

    @JsonProperty("stream_name")
    @NotNull
    @Valid
    private String name;

    @JsonProperty("stream_arn")
    private String arn;

    @JsonProperty("initial_position")
    private InitialPositionInStream initialPosition = InitialPositionInStream.LATEST;

    @JsonProperty("checkpoint_interval")
    private int checkPointIntervalInMilliseconds = MINIMAL_CHECKPOINT_INTERVAL_MILLIS;

    @Getter
    @JsonProperty("enableCheckpoint")
    private boolean enableCheckPoint = DEFAULT_ENABLE_CHECKPOINT;
}
