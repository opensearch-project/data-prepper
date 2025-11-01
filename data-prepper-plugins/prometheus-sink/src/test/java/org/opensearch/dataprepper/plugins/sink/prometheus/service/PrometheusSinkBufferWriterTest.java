/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.common.sink.SinkMetrics;

import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Instant;

public class PrometheusSinkBufferWriterTest {
    @Mock
    private SinkMetrics sinkMetrics;
    @Mock
    private PrometheusSinkFlushContext sinkFlushContext;

    private JacksonGauge gauge;
    private PrometheusSinkBufferEntry prometheusSinkBufferEntry;
    private PrometheusSinkBufferWriter prometheusSinkBufferWriter;
    
    
    @BeforeEach
    void setUp() throws Exception {
        sinkMetrics = mock(SinkMetrics.class);
        sinkFlushContext = mock(PrometheusSinkFlushContext.class);
        gauge = createGaugeMetric();
        prometheusSinkBufferEntry = new PrometheusSinkBufferEntry(gauge);
    }

    PrometheusSinkBufferWriter createObjectUnderTest() {
        return new PrometheusSinkBufferWriter(sinkMetrics);
    }

    @Test
    public void testPrometheusSinkBufferWriter() throws Exception {
        prometheusSinkBufferWriter = createObjectUnderTest();
        prometheusSinkBufferWriter.writeToBuffer(prometheusSinkBufferEntry);
        PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        assertThat(prometheusSinkFlushableBuffer.getEvents().get(0), sameInstance(gauge));
    }

    private JacksonGauge createGaugeMetric() {
        return JacksonGauge.builder()
            .withName("gauge")
            .withDescription("Test Gauge Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withValue(1.0d)
            .build(false);
    }

}
