 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkThresholdConfig;

import org.mockito.Mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class PrometheusSinkBufferWriterTest {
    @Mock
    private SinkMetrics sinkMetrics;
    @Mock
    private PrometheusSinkFlushContext sinkFlushContext;
    @Mock
    private PrometheusSinkConfiguration sinkConfig;
    @Mock
    private PrometheusSinkThresholdConfig sinkThresholdConfig;

    private JacksonGauge gauge1;
    private PrometheusSinkBufferEntry prometheusSinkBufferEntry;
    private PrometheusSinkBufferWriter prometheusSinkBufferWriter;


    @BeforeEach
    void setUp() throws Exception {
        sinkMetrics = mock(SinkMetrics.class);
        sinkThresholdConfig = mock(PrometheusSinkThresholdConfig.class);
        sinkConfig = mock(PrometheusSinkConfiguration.class);

        when(sinkThresholdConfig.getMaxEvents()).thenReturn(3);
        when(sinkThresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(sinkConfig.getThresholdConfig()).thenReturn(sinkThresholdConfig);
        when(sinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(0));
        sinkFlushContext = mock(PrometheusSinkFlushContext.class);
        gauge1 = createGaugeMetric("gauge1", Instant.now(), 1.0d);
        prometheusSinkBufferEntry = new PrometheusSinkBufferEntry(gauge1, true);
    }

    PrometheusSinkBufferWriter createObjectUnderTest() {
        return new PrometheusSinkBufferWriter(sinkConfig, sinkMetrics);
    }

    @Test
    public void testPrometheusSinkBufferWriter() throws Exception {
        prometheusSinkBufferWriter = createObjectUnderTest();
        prometheusSinkBufferWriter.writeToBuffer(prometheusSinkBufferEntry);
        assertThat(prometheusSinkBufferWriter.getBufferSize(), equalTo(1L));
        PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        assertThat(prometheusSinkFlushableBuffer.getEvents().get(0), sameInstance(gauge1));
        assertThat(prometheusSinkBufferWriter.getBufferSize(), equalTo(0L));
    }

    @Test
    public void testPrometheusSinkBufferWriterWithDuplicateTimeEntries() throws Exception {
        prometheusSinkBufferWriter = createObjectUnderTest();
        Instant t2 = Instant.now().plusSeconds(5);
        // Same metric with same name but different value, only most recent one is kept
        Gauge gauge2 = createGaugeMetric("gauge2", t2, 10.0d);
        Gauge gauge3 = createGaugeMetric("gauge2", t2, 20.0d);
        PrometheusSinkBufferEntry entry2 = new PrometheusSinkBufferEntry(gauge2, true);
        PrometheusSinkBufferEntry entry3 = new PrometheusSinkBufferEntry(gauge3, true);
        prometheusSinkBufferWriter.writeToBuffer(prometheusSinkBufferEntry);
        prometheusSinkBufferWriter.writeToBuffer(entry2);
        prometheusSinkBufferWriter.writeToBuffer(entry3);
        assertThat(prometheusSinkBufferWriter.getBufferSize(), equalTo(2L));
        PrometheusSinkFlushContext sinkFlushContext = mock(PrometheusSinkFlushContext.class);
        PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        assertThat(prometheusSinkFlushableBuffer.getEvents().size(), equalTo(2));
        Event ev1 = prometheusSinkFlushableBuffer.getEvents().get(0);
        Event ev2 = prometheusSinkFlushableBuffer.getEvents().get(1);
        assertTrue(ev1 == gauge1 || ev1 == gauge3);
        assertTrue(ev2 == gauge1 || ev2 == gauge3);
    }

    @Test
    public void testPrometheusSinkBufferWriterWithOutOfOrderEntries() throws Exception {
        prometheusSinkBufferWriter = createObjectUnderTest();
        // Same metric with same name but different value and different times but out of order times
        // Expected result is sorted by time
        Instant t2 = Instant.now().plusSeconds(50);
        Gauge gauge2 = createGaugeMetric("gauge1", t2, 10.0d);
        Instant t3 = t2.minusSeconds(150);
        Gauge gauge3 = createGaugeMetric("gauge1", t3, 20.0d);
        PrometheusSinkBufferEntry entry2 = new PrometheusSinkBufferEntry(gauge2, true);
        PrometheusSinkBufferEntry entry3 = new PrometheusSinkBufferEntry(gauge3, true);
        prometheusSinkBufferWriter.writeToBuffer(prometheusSinkBufferEntry);
        prometheusSinkBufferWriter.writeToBuffer(entry2);
        prometheusSinkBufferWriter.writeToBuffer(entry3);
        PrometheusSinkFlushContext sinkFlushContext = mock(PrometheusSinkFlushContext.class);
        PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        assertThat(prometheusSinkFlushableBuffer.getEvents().size(), equalTo(3));
        assertThat(prometheusSinkFlushableBuffer.getEvents().get(0), sameInstance(gauge3));
        assertThat(prometheusSinkFlushableBuffer.getEvents().get(1), sameInstance(gauge1));
        assertThat(prometheusSinkFlushableBuffer.getEvents().get(2), sameInstance(gauge2));

    }

    @Test
    public void testGetBufferWithMultipleMetrics() throws Exception {
        when(sinkThresholdConfig.getMaxEvents()).thenReturn(5);
        when(sinkThresholdConfig.getMaxRequestSizeBytes()).thenReturn(100000L);
        when(sinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(3));
        prometheusSinkBufferWriter = createObjectUnderTest();
        Instant t1 = Instant.now();
        Instant t2 = t1.minusSeconds(3);
        for (int i = 0; i < 5; i++) {
            Gauge gauge1 = createGaugeMetric("gauge_"+i, t1, 20.0d+i);
            Gauge gauge2 = createGaugeMetric("gauge_"+i, t2, 30.0d+i);
            PrometheusSinkBufferEntry entry1 = new PrometheusSinkBufferEntry(gauge1, false);
            PrometheusSinkBufferEntry entry2 = new PrometheusSinkBufferEntry(gauge2, false);
            prometheusSinkBufferWriter.writeToBuffer(entry1);
            prometheusSinkBufferWriter.writeToBuffer(entry2);
        }
        PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        List<Event> events = prometheusSinkFlushableBuffer.getEvents();
        assertThat(events.size(), equalTo(5));
        Map<String, Double> expectedValues = Map.of("gauge_0", 30.0d, "gauge_1", 31.0d, "gauge_2", 32.0d, "gauge_3", 33.0d, "gauge_4", 34.0d);
        for (int i = 0; i < events.size(); i++) {
            assertTrue(events.get(i) instanceof Metric);
            assertTrue(events.get(i) instanceof Gauge);
            Gauge gauge = (Gauge)events.get(i);
            Double d = expectedValues.get(gauge.getName());

            assertTrue(d != null);
            assertThat(d, equalTo(gauge.getValue()));
        }
        try {
            Thread.sleep(4000);
        } catch (Exception e){}

        prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        events = prometheusSinkFlushableBuffer.getEvents();
        assertThat(events.size(), equalTo(5));
        expectedValues = Map.of("gauge_0", 20.0d, "gauge_1", 21.0d, "gauge_2", 22.0d, "gauge_3", 23.0d, "gauge_4", 24.0d);
        for (int i = 0; i < events.size(); i++) {
            assertTrue(events.get(i) instanceof Metric);
            assertTrue(events.get(i) instanceof Gauge);
            Gauge gauge = (Gauge)events.get(i);
            Double d = expectedValues.get(gauge.getName());

            assertTrue(d != null);
            assertThat(d, equalTo(gauge.getValue()));
        }
    }

    private JacksonGauge createGaugeMetric(final String name, final Instant time, final double value) {
        return JacksonGauge.builder()
            .withName(name)
            .withDescription("Test Gauge Metric")
            .withTimeReceived(time)
            .withTime(time.plusSeconds(10).toString())
            .withStartTime(time.plusSeconds(5).toString())
            .withUnit("1")
            .withValue(value)
            .build(false);
    }

}
