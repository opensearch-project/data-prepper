/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.SinkFlushResult;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;

import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import software.amazon.awssdk.utils.Pair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class PrometheusSinkFlushableBufferTest {
    @Mock
    private List<SinkBufferEntry> buffer;
    @Mock
    private SinkMetrics sinkMetrics;
    @Mock
    private PrometheusSinkFlushContext sinkFlushContext;
    @Mock
    private PrometheusHttpSender httpSender;
    
    private PrometheusSinkFlushableBuffer prometheusSinkFlushableBuffer;

    @BeforeEach
    void setUp() throws Exception {
        JacksonGauge gauge = createGaugeMetric("gauge1");
        PrometheusSinkBufferEntry bufferEntry1 = new PrometheusSinkBufferEntry(gauge, true);
        gauge = createGaugeMetric("gauge2");
        PrometheusSinkBufferEntry bufferEntry2 = new PrometheusSinkBufferEntry(gauge, true);
        buffer = new ArrayList<>();
        buffer.add((SinkBufferEntry)bufferEntry1);
        buffer.add((SinkBufferEntry)bufferEntry2);
        httpSender = mock(PrometheusHttpSender.class);
        sinkMetrics = mock(SinkMetrics.class);
        sinkFlushContext = mock(PrometheusSinkFlushContext.class);
        when(sinkFlushContext.getHttpSender()).thenReturn(httpSender);
    }

    PrometheusSinkFlushableBuffer createObjectUnderTest() {
        return new PrometheusSinkFlushableBuffer(buffer, sinkMetrics, sinkFlushContext);
    }

    @Test
    public void testPrometheusSinkFlushableBufferWithSuccess() throws Exception {
        when(httpSender.pushToEndPoint(any())).thenReturn(Pair.of(true, 0));
        prometheusSinkFlushableBuffer = createObjectUnderTest();
        List<Event> events = prometheusSinkFlushableBuffer.getEvents();
        assertThat(events.size(), equalTo(buffer.size()));
        SinkFlushResult result = prometheusSinkFlushableBuffer.flush();
        assertThat(result, equalTo(null));
        verify(sinkMetrics, times(1)).incrementRequestsSuccessCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsSuccessCounter(any(Integer.class));
    }

    @Test
    public void testPrometheusSinkFlushableBufferWithFailure() throws Exception {
        when(httpSender.pushToEndPoint(any())).thenReturn(Pair.of(false, 404));
        prometheusSinkFlushableBuffer = createObjectUnderTest();
        List<Event> events = prometheusSinkFlushableBuffer.getEvents();
        assertThat(events.size(), equalTo(buffer.size()));
        SinkFlushResult result = prometheusSinkFlushableBuffer.flush();
        assertThat(result, notNullValue());
        assertThat(result.getException(), equalTo(null));
        assertThat(result.getStatusCode(), equalTo(404));
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
    }

    private JacksonGauge createGaugeMetric(final String name) {
        return JacksonGauge.builder()
            .withName(name)
            .withDescription("Test Gauge Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withValue(1.0d)
            .build(false);
    }

}

