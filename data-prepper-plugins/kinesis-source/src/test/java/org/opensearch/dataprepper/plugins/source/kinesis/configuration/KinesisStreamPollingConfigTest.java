package org.opensearch.dataprepper.plugins.source.kinesis.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KinesisStreamPollingConfigTest {
    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final int IDLE_TIME_BETWEEN_READS_IN_MILLIS = 250;

    @Test
    void testConfig() {
        KinesisStreamPollingConfig kinesisStreamPollingConfig = new KinesisStreamPollingConfig();
        assertEquals(kinesisStreamPollingConfig.getMaxPollingRecords(), DEFAULT_MAX_RECORDS);
        assertEquals(kinesisStreamPollingConfig.getIdleTimeBetweenReadsInMillis(), IDLE_TIME_BETWEEN_READS_IN_MILLIS);
    }

}
