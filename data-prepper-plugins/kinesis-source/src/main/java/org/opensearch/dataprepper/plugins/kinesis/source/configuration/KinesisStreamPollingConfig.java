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
import lombok.Getter;

import java.time.Duration;

public class KinesisStreamPollingConfig {
    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final Duration IDLE_TIME_BETWEEN_READS = Duration.ofMillis(250);
    @Getter
    @JsonProperty("max_polling_records")
    private int maxPollingRecords = DEFAULT_MAX_RECORDS;

    @Getter
    @JsonProperty("idle_time_between_reads")
    private Duration idleTimeBetweenReads = IDLE_TIME_BETWEEN_READS;

}
