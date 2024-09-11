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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KinesisStreamPollingConfigTest {
    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final int IDLE_TIME_BETWEEN_READS_IN_MILLIS = 250;

    @Test
    void testConfig() {
        KinesisStreamPollingConfig kinesisStreamPollingConfig = new KinesisStreamPollingConfig();
        assertEquals(kinesisStreamPollingConfig.getMaxPollingRecords(), DEFAULT_MAX_RECORDS);
        assertEquals(kinesisStreamPollingConfig.getIdleTimeBetweenReads(), Duration.ofMillis(IDLE_TIME_BETWEEN_READS_IN_MILLIS));
    }

}
