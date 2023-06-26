package org.opensearch.dataprepper.plugins.sink.threshold;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

public class ThresholdCheckTest {
    private ThresholdCheck thresholdCheck;

    @BeforeEach
    void setUp() {
        thresholdCheck = new ThresholdCheck(ThresholdConfig.DEFAULT_BATCH_SIZE, ThresholdConfig.DEFAULT_EVENT_SIZE,
                ThresholdConfig.DEFAULT_SIZE_OF_REQUEST, ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, })
    void check_batchSize_valid() {

    }
}
