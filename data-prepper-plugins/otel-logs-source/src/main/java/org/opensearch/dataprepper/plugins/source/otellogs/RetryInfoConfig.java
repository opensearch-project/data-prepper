package org.opensearch.dataprepper.plugins.source.otellogs;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RetryInfoConfig {

    @JsonProperty("min_delay")
    private Duration minDelay;

    @JsonProperty("max_delay")
    private Duration maxDelay;

    // Jackson needs this constructor
    public RetryInfoConfig() {}

    public RetryInfoConfig(Duration minDelay, Duration maxDelay) {
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
    }

    public Duration getMinDelay() {
        return minDelay;
    }

    public void setMinDelay(Duration minDelay) {
        this.minDelay = minDelay;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(Duration maxDelay) {
        this.maxDelay = maxDelay;
    }
}
