/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * With more than one process worker, only the master instance evaluates the windows. It must read every
 * trace in the window, not just its own key-range segment (issue #7164).
 */
class OTelApmServiceMapProcessorMultiWorkerTest {
    private static final int TRACES = 200;
    private static final Instant T0 = Instant.ofEpochSecond(1_609_459_200L);

    @TempDir
    File tempDir;

    private volatile EventMetadata eventMetadata;
    private volatile Object eventData;

    @BeforeEach
    void resetSharedState() {
        OTelApmServiceMapProcessor.resetSharedStateForTesting();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void everyTraceIsEvaluatedWhateverTheWorkerCount(final int workers) throws Exception {
        final MutableClock clock = new MutableClock(T0);
        final EventFactory eventFactory = jacksonEventFactory();
        // Dependency nodes are on by default, so the processor registers its overflow counters.
        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
        final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        when(pluginMetrics.counter(any())).thenReturn(mock(Counter.class));
        when(pluginMetrics.timer(any())).thenAnswer(a -> meterRegistry.timer(a.getArgument(0)));
        when(pluginMetrics.summary(any())).thenAnswer(a -> meterRegistry.summary(a.getArgument(0)));
        final List<OTelApmServiceMapProcessor> processors = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            processors.add(new OTelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, workers,
                    eventFactory, pluginMetrics));
        }
        try {
            // One leaf SERVER span per trace, each for a distinct service, with random trace ids spread over
            // the key range, split across the workers as the pipeline does.
            final Random random = new Random(42);
            final List<List<Record<Event>>> batches = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                batches.add(new ArrayList<>());
            }
            for (int t = 0; t < TRACES; t++) {
                final byte[] traceId = new byte[16];
                random.nextBytes(traceId);
                batches.get(t % workers).add(new Record<>(leafServerSpan("svc-" + t, hex(traceId))));
            }

            // t=0: spans land in the next window; t=65s: rotate; t=130s: the window holding them is evaluated.
            tick(processors, batches);
            clock.set(T0.plusSeconds(65));
            tick(processors, emptyBatches(workers));
            clock.set(T0.plusSeconds(130));
            final List<Record<Event>> emitted = tick(processors, emptyBatches(workers));

            final Set<String> evaluatedServices = ConcurrentHashMap.newKeySet();
            for (final Record<Event> record : emitted) {
                final Event event = record.getData();
                if (event.getMetadata() != null && "SERVICE_MAP".equals(event.getMetadata().getEventType())) {
                    evaluatedServices.add(event.get("sourceNode/keyAttributes/name", String.class));
                }
            }
            assertThat("workers=" + workers, evaluatedServices.size(), equalTo(TRACES));

            // One evaluation per window, timed, and the evaluation that emitted them saw every trace.
            final Timer evaluationTime = meterRegistry.timer(OTelApmServiceMapProcessor.WINDOW_EVALUATION_TIME_METRIC);
            final DistributionSummary evaluatedTraces =
                    meterRegistry.summary(OTelApmServiceMapProcessor.WINDOW_EVALUATION_TRACES_METRIC);
            final DistributionSummary evaluatedSpans =
                    meterRegistry.summary(OTelApmServiceMapProcessor.WINDOW_EVALUATION_SPANS_METRIC);
            assertThat(evaluationTime.count(), equalTo(2L));
            assertThat(evaluatedTraces.max(), equalTo((double) TRACES));
            assertThat(evaluatedSpans.max(), equalTo((double) TRACES));
        } finally {
            processors.get(0).shutdown();
        }
    }

    /** Runs one doExecute per worker concurrently; the processor's barrier requires all workers. */
    private static List<Record<Event>> tick(final List<OTelApmServiceMapProcessor> processors,
                                            final List<List<Record<Event>>> batches) throws Exception {
        final List<Record<Event>> results = Collections.synchronizedList(new ArrayList<>());
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < processors.size(); i++) {
            final OTelApmServiceMapProcessor processor = processors.get(i);
            final List<Record<Event>> batch = batches.get(i);
            threads.add(new Thread(() -> {
                try {
                    results.addAll(processor.doExecute(batch));
                } catch (final Throwable e) {
                    failure.set(e);
                }
            }));
        }
        threads.forEach(Thread::start);
        for (final Thread thread : threads) {
            thread.join(30_000);
            assertThat("worker still running after 30s (barrier hang?)", thread.isAlive(), equalTo(false));
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        return results;
    }

    private static List<List<Record<Event>>> emptyBatches(final int workers) {
        final List<List<Record<Event>>> batches = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            batches.add(Collections.emptyList());
        }
        return batches;
    }

    private EventFactory jacksonEventFactory() {
        final EventFactory eventFactory = mock(EventFactory.class);
        final BaseEventBuilder<Event> eventBuilder = mock(EventBuilder.class, RETURNS_DEEP_STUBS);
        when(eventFactory.eventBuilder(any())).thenReturn(eventBuilder);
        doAnswer(a -> {
            eventMetadata = a.getArgument(0);
            return eventBuilder;
        }).when(eventBuilder).withEventMetadata(any());
        doAnswer(a -> {
            eventData = a.getArgument(0);
            return eventBuilder;
        }).when(eventBuilder).withData(any());
        doAnswer(a -> JacksonEvent.builder().withEventMetadata(eventMetadata).withData(eventData).build())
                .when(eventBuilder).build();
        return eventFactory;
    }

    private static Span leafServerSpan(final String serviceName, final String traceId) {
        final Span span = mock(Span.class);
        lenient().when(span.getServiceName()).thenReturn(serviceName);
        lenient().when(span.getSpanId()).thenReturn(traceId.substring(0, 16));
        lenient().when(span.getParentSpanId()).thenReturn("");
        lenient().when(span.getTraceId()).thenReturn(traceId);
        lenient().when(span.getKind()).thenReturn("SPAN_KIND_SERVER");
        lenient().when(span.getName()).thenReturn("GET /");
        lenient().when(span.getDurationInNanos()).thenReturn(1_000_000L);
        lenient().when(span.getEndTime()).thenReturn("2021-01-01T00:00:00.000Z");
        final Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        lenient().when(span.getStatus()).thenReturn(status);
        lenient().when(span.getAttributes()).thenReturn(Collections.emptyMap());
        lenient().when(span.getResource()).thenReturn(Collections.emptyMap());
        return span;
    }

    private static String hex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static final class MutableClock extends Clock {
        private volatile Instant now;

        private MutableClock(final Instant now) {
            this.now = now;
        }

        void set(final Instant instant) {
            this.now = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
