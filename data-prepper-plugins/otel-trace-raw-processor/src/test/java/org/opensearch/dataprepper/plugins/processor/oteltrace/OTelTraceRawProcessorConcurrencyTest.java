/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A root and a child of one trace processed by two workers at the same time must both be emitted
 * exactly once, whatever the interleaving: the root caches the trace group and then atomically
 * claims the waiting span set, and a child that inserted itself too late for that claim re-checks
 * the cache and claims itself.
 */
class OTelTraceRawProcessorConcurrencyTest {

    private static final int ITERATIONS = 200;

    @Test
    void test_when_rootAndChildAreProcessedConcurrently_then_everySpanIsEmittedExactlyOnceWithItsTraceGroup() throws Exception {
        final OTelTraceRawProcessor processor = createProcessor();
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                final String traceId = "race-trace-" + iteration;
                final String traceGroupName = "trace-group-" + iteration;
                final String rootSpanId = "root-" + iteration;
                final String childSpanId = "child-" + iteration;
                final Span rootSpan = rootSpan(traceId, rootSpanId, traceGroupName);
                final Span childSpan = childSpan(traceId, childSpanId, rootSpanId);

                final CountDownLatch startLatch = new CountDownLatch(1);
                final Future<Collection<Record<Span>>> rootFuture = executorService.submit(() -> {
                    startLatch.await();
                    return processor.doExecute(List.of(new Record<>(rootSpan)));
                });
                final Future<Collection<Record<Span>>> childFuture = executorService.submit(() -> {
                    startLatch.await();
                    return processor.doExecute(List.of(new Record<>(childSpan)));
                });
                startLatch.countDown();

                final List<Span> emittedSpans = new ArrayList<>();
                rootFuture.get(10, TimeUnit.SECONDS).forEach(record -> emittedSpans.add(record.getData()));
                childFuture.get(10, TimeUnit.SECONDS).forEach(record -> emittedSpans.add(record.getData()));

                final List<String> emittedSpanIds = emittedSpans.stream()
                        .map(Span::getSpanId)
                        .sorted()
                        .collect(Collectors.toList());
                assertThat("iteration " + iteration, emittedSpanIds, equalTo(List.of(childSpanId, rootSpanId)));

                final Span emittedChildSpan = emittedSpans.stream()
                        .filter(span -> span.getSpanId().equals(childSpanId))
                        .findFirst().orElseThrow();
                assertThat("iteration " + iteration, emittedChildSpan.getTraceGroup(), equalTo(traceGroupName));
            }
        } finally {
            executorService.shutdownNow();
        }

        assertThat(processor.isReadyForShutdown(), is(true));
    }

    private static OTelTraceRawProcessor createProcessor() {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), mock(PluginMetrics.class));
    }

    private static Span rootSpan(final String traceId, final String spanId, final String traceGroupName) {
        return spanBuilder(traceId, spanId, "")
                .withTraceGroup(traceGroupName)
                .withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode(0)
                        .withEndTime("2026-01-01T00:01:00Z")
                        .withDurationInNanos(1L)
                        .build())
                .build();
    }

    private static Span childSpan(final String traceId, final String spanId, final String parentSpanId) {
        return spanBuilder(traceId, spanId, parentSpanId)
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .build();
    }

    private static JacksonSpan.Builder spanBuilder(final String traceId, final String spanId, final String parentSpanId) {
        return JacksonSpan.builder()
                .withTraceId(traceId)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withTraceState("")
                .withName(spanId)
                .withKind("INTERNAL")
                .withDurationInNanos(1L)
                .withStartTime("2026-01-01T00:00:00Z")
                .withEndTime("2026-01-01T00:01:00Z")
                .withAttributes(new HashMap<>(Map.of("http.method", "POST")));
    }
}
