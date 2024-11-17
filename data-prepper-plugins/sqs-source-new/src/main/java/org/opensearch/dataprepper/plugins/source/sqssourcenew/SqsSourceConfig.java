package org.opensearch.dataprepper.plugins.source.sqssourcenew;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;

public class SqsSourceConfig {

    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_BUFFER_TIMEOUT;

    @JsonProperty("records_to_accumulate")
    private int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

    @JsonProperty("queues")
    @NotNull
    @Valid
    private List<QueueConfig> queues;

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public int getNumberOfRecordsToAccumulate() {
        return numberOfRecordsToAccumulate;
    }

    public boolean getAcknowledgements() {
        return acknowledgments;
    }

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    public List<QueueConfig> getQueues() {
        return queues;
    }
}
