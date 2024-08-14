package org.opensearch.dataprepper.plugins.source.kinesis.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

public class KinesisStreamPollingConfig {
    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final int IDLE_TIME_BETWEEN_READS_IN_MILLIS = 250;
    @Getter
    @JsonProperty("maxPollingRecords")
    private int maxPollingRecords = DEFAULT_MAX_RECORDS;

    @Getter
    @JsonProperty("idleTimeBetweenReadsInMillis")
    private int idleTimeBetweenReadsInMillis = IDLE_TIME_BETWEEN_READS_IN_MILLIS;

}
