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
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.event.Event;

import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Instant;

public class PrometheusSinkBufferWriterTest {
    @Mock
    private SinkMetrics sinkMetrics;
    @Mock
    private PrometheusSinkFlushContext sinkFlushContext;

    private JacksonGauge gauge1;
    private PrometheusSinkBufferEntry prometheusSinkBufferEntry;
    private PrometheusSinkBufferWriter prometheusSinkBufferWriter;


    @BeforeEach
    void setUp() throws Exception {
        sinkMetrics = mock(SinkMetrics.class);
        sinkFlushContext = mock(PrometheusSinkFlushContext.class);
        gauge1 = createGaugeMetric("gauge1", Instant.now(), 1.0d);
        prometheusSinkBufferEntry = new PrometheusSinkBufferEntry(gauge1, true);
    }

    PrometheusSinkBufferWriter createObjectUnderTest() {
        return new PrometheusSinkBufferWriter(sinkMetrics);
    }

    @Test
    public void testPrometheusSinkBufferWriter() throws Exception {
        prometheusSinkBufferWriter = createObjectUnderTest();
        prometheusSinkBufferWriter.writeToBuffer(prometheusSinkBufferEntry);
        PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer = (PrometheusSinkFlushableBuffer)prometheusSinkBufferWriter.getBuffer(sinkFlushContext);
        assertThat(prometheusSinkFlushableBuffer.getEvents().get(0), sameInstance(gauge1));
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
