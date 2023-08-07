package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;

import java.time.Duration;

public class S3ScanSchedulingOptions {
    @JsonProperty("rate")
    private Duration rate = Duration.ofHours(8);

    @Min(1)
    @JsonProperty("count")
    private int Count = 1;

    public Duration getRate() {
        return rate;
    }

    public int getCount() {
        return Count;
    }

}
