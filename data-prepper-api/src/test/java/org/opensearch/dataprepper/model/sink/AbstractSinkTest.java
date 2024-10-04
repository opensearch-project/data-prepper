/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractSinkTest {
    private String sinkName;
    private String pipelineName;
    private PluginSetting pluginSetting;

    @BeforeEach
    void setUp() {
        sinkName = "testSink";
        pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();
        pluginSetting = new PluginSetting(sinkName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
    }

    @Test
    void testMetrics() {
        AbstractSink<Record<String>> abstractSink = new AbstractSinkImpl(pluginSetting);
        abstractSink.initialize();
        assertEquals(abstractSink.isReady(), true);
        abstractSink.updateLatencyMetrics(Arrays.asList(
                new Record<>(UUID.randomUUID().toString())));
        abstractSink.output(Arrays.asList(
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString())
        ));

        final List<Measurement> recordsInMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(sinkName).add(MetricNames.RECORDS_IN).toString());
        final List<Measurement> elapsedTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(sinkName).add(MetricNames.TIME_ELAPSED).toString());

        assertEquals(1, recordsInMeasurements.size());
        assertEquals(5.0, recordsInMeasurements.get(0).getValue(), 0);
        assertEquals(3, elapsedTimeMeasurements.size());
        assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.COUNT).getValue(), 0);
        double totalTime = MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.TOTAL_TIME).getValue();
        assertAll(
                () -> assertThat(totalTime, greaterThan(0.1)),
                () -> assertThat(totalTime, lessThan(0.2))
        );
        assertEquals(abstractSink.getRetryThreadState(), null);
        abstractSink.shutdown();
    }

    @Test
    void testSinkNotReady() throws InterruptedException {
        AbstractSinkNotReadyImpl abstractSink = new AbstractSinkNotReadyImpl(pluginSetting);
        abstractSink.initialize();
        assertEquals(abstractSink.isReady(), false);
        assertEquals(abstractSink.getRetryThreadState(), Thread.State.RUNNABLE);

        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(abstractSink.initCount, greaterThanOrEqualTo(1)));

        abstractSink.initialized = true;
        assertEquals(abstractSink.isReady(), true);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(abstractSink.getRetryThreadState(), equalTo(Thread.State.TERMINATED)));
        assertThat(abstractSink.getRetryThreadState(), equalTo(Thread.State.TERMINATED));
        int initCountBeforeShutdown = abstractSink.initCount;
        abstractSink.shutdown();
        Thread.sleep(200);
        assertThat(abstractSink.initCount, equalTo(initCountBeforeShutdown));
    }

    @Test
    void testSinkWithRegisterEventReleaseHandler() {
        final AbstractSink<Record<Event>> abstractSink = new AbstractEventSinkImpl(pluginSetting);
        abstractSink.initialize();
        assertEquals(abstractSink.isReady(), true);

        final Event event = mock(Event.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        final int minimumLatencySeconds = 20;
        final Instant startTime = Instant.now().minus(Duration.ofSeconds(minimumLatencySeconds));
        when(eventHandle.getInternalOriginationTime()).thenReturn(startTime);
        when(event.getEventHandle()).thenReturn(eventHandle);
        doAnswer(a -> {
            final BiConsumer<EventHandle, Boolean> onReleaseHandler = a.getArgument(0, BiConsumer.class);
            onReleaseHandler.accept(eventHandle, true);
            return a;
        }).when(eventHandle).onRelease(any());
        final Record record = mock(Record.class);
        when(record.getData()).thenReturn(event);

        abstractSink.updateLatencyMetrics(Arrays.asList(record));
        abstractSink.output(Arrays.asList(record));
        abstractSink.shutdown();

        final List<Measurement> latencyMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(sinkName).add(SinkLatencyMetrics.INTERNAL_LATENCY).toString());
        assertThat(latencyMeasurements.size(), greaterThanOrEqualTo(1));
        double totalTime = MetricsTestUtil.getMeasurementFromList(latencyMeasurements, Statistic.TOTAL_TIME).getValue();
        assertAll(
                () -> assertThat(totalTime, greaterThan((double)minimumLatencySeconds)),
                () -> assertThat(totalTime, lessThan( minimumLatencySeconds + 1.0))
        );
    }

    private static class AbstractEventSinkImpl extends AbstractSink<Record<Event>> {

        AbstractEventSinkImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public void doOutput(Collection<Record<Event>> records) {
            for (final Record<Event> record: records) {
                Event event = record.getData();
                event.getEventHandle().release(true);
            }
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public void doInitialize() {
        }

        @Override
        public boolean isReady() {
            return true;
        }
    }


    private static class AbstractSinkImpl extends AbstractSink<Record<String>> {

        AbstractSinkImpl(PluginSetting pluginSetting) {
            super(pluginSetting, 10, 10);
        }

        @Override
        public void doOutput(Collection<Record<String>> records) {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {

            }
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public void doInitialize() {
        }

        @Override
        public boolean isReady() {
            return true;
        }
    }

    private static class AbstractSinkNotReadyImpl extends AbstractSink<Record<String>> {

        boolean initialized;
        int initCount;
        AbstractSinkNotReadyImpl(PluginSetting pluginSetting) {
            super(pluginSetting, 100, 20);
            initialized = false;
            initCount = 0;
        }

        @Override
        public void doOutput(Collection<Record<String>> records) {
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public void doInitialize() {
            // make this check for smaller number so that test finishes sooner
            if (initCount++ == DEFAULT_MAX_RETRIES/200) {
                initialized = true;
            }
        }

        @Override
        public boolean isReady() {
            return initialized;
        }
    }
}
