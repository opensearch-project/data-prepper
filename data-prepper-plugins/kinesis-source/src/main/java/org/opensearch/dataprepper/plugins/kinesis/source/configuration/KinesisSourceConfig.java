package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;
import java.util.List;

public class KinesisSourceConfig {
    static final Duration DEFAULT_TIME_OUT_IN_MILLIS = Duration.ofMillis(1000);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;
    static final Duration DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT = Duration.ofMinutes(10);

    @Getter
    @JsonProperty("streams")
    @NotNull
    @Valid
    @Size(min = 1, max = 4, message = "Only support a maximum of 4 streams")
    private List<KinesisStreamConfig> streams;

    @Getter
    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @Getter
    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_TIME_OUT_IN_MILLIS;

    @Getter
    @JsonProperty("records_to_accumulate")
    private int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

    @JsonProperty("acknowledgments")
    @Getter
    private boolean acknowledgments = false;

    @Getter
    @JsonProperty("consumer_strategy")
    private ConsumerStrategy consumerStrategy = ConsumerStrategy.ENHANCED_FAN_OUT;

    @Getter
    @JsonProperty("polling")
    private KinesisStreamPollingConfig pollingConfig;

    @Getter
    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("shard_acknowledgment_timeout")
    private Duration shardAcknowledgmentTimeout = DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT;

    public Duration getShardAcknowledgmentTimeout() {
        return shardAcknowledgmentTimeout;
    }
}



