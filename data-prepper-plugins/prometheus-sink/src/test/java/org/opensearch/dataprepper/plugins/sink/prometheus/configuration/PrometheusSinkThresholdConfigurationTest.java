 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.util.Random;

public class PrometheusSinkThresholdConfigurationTest {
    private PrometheusSinkThresholdConfig thresholdConfig;

    PrometheusSinkThresholdConfig createObjectUnderTest() {
        return new PrometheusSinkThresholdConfig();
    }

    @Test
    void prometheus_sink_default_threshold_config_test() {
        thresholdConfig = createObjectUnderTest();
        assertThat(thresholdConfig.getMaxEvents(), equalTo(PrometheusSinkThresholdConfig.DEFAULT_MAX_EVENTS));
        assertThat(thresholdConfig.getMaxRequestSizeBytes(), equalTo(ByteCount.parse(PrometheusSinkThresholdConfig.DEFAULT_MAX_REQUEST_SIZE).getBytes()));
        assertThat(thresholdConfig.getFlushInterval(), equalTo(PrometheusSinkThresholdConfig.DEFAULT_FLUSH_INTERVAL_SECONDS));
    }

    @Test
    void prometheus_sink_custom_threshold_config_test() throws NoSuchFieldException, IllegalAccessException {
        thresholdConfig = createObjectUnderTest();
        Random random = new Random();
        int testMaxEvents = random.nextInt(10) + 1;
        ReflectivelySetField.setField(PrometheusSinkThresholdConfig.class, thresholdConfig, "maxEvents", testMaxEvents);
        long testMaxRequestSize = random.nextInt(10)  + 1L;
        ReflectivelySetField.setField(PrometheusSinkThresholdConfig.class, thresholdConfig, "maxRequestSize", testMaxRequestSize+"mb");
        long testFlushInterval = (long)random.nextInt(10) + 1L;
        ReflectivelySetField.setField(PrometheusSinkThresholdConfig.class, thresholdConfig, "flushInterval", Duration.ofSeconds(testFlushInterval));
        assertThat(thresholdConfig.getMaxEvents(), equalTo(testMaxEvents));
        assertThat(thresholdConfig.getMaxRequestSizeBytes(), equalTo(testMaxRequestSize * 1024*1024L));
        assertThat(thresholdConfig.getFlushInterval(), equalTo(testFlushInterval));
    }

}

