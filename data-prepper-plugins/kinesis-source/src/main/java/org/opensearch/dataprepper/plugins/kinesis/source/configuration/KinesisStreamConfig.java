package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.time.Duration;

@Getter
public class KinesisStreamConfig {
    // Checkpointing interval
    private static final Duration MINIMAL_CHECKPOINT_INTERVAL = Duration.ofMillis(2 * 60 * 1000); // 2 minute
    private static final boolean DEFAULT_ENABLE_CHECKPOINT = false;

    @JsonProperty("stream_name")
    @NotNull
    @Valid
    private String name;

    @JsonProperty("initial_position")
    private InitialPositionInStreamConfig initialPosition = InitialPositionInStreamConfig.LATEST;

    @JsonProperty("checkpoint_interval")
    private Duration checkPointInterval = MINIMAL_CHECKPOINT_INTERVAL;

    @Getter
    @JsonProperty("enable_checkpoint")
    private boolean enableCheckPoint = DEFAULT_ENABLE_CHECKPOINT;

    public InitialPositionInStream getInitialPosition() {
        return initialPosition.getPositionInStream();
    }
}
