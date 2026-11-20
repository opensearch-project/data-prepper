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
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * A root and a child of one trace processed by two workers at the same time must both be emitted
 * exactly once, whatever the interleaving: the root caches the trace group and then atomically
 * claims the waiting span set, and a child that inserted itself too late for that claim re-checks
 * the cache and claims itself. A GenAI root is recorded in the trace state before its trace group
 * is cached, so a child that sees the trace group also sees its GenAI parent and is not marked as
 * a second AI root.
 */
class OTelTraceRawProcessorConcurrencyTest {

    private static final int ITERATIONS = 200;
    private static final int GENAI_ITERATIONS = 500;

    @Test
    void test_when_rootAndChildAreProcessedConcurrently_then_everySpanIsEmittedExactlyOnceWithItsTraceGroup() throws Exception {
        final OTelTraceRawProcessor processor = createProcessor(Duration.ZERO);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                final String traceId = "race-trace-" + iteration;
                final String traceGroupName = "trace-group-" + iteration;
                final String rootSpanId = "root-" + iteration;
                final String childSpanId = "child-" + iteration;
                final Span rootSpan = rootSpan(traceId, rootSpanId, traceGroupName, Map.of("http.method", "POST"));
                final Span childSpan = childSpan(traceId, childSpanId, rootSpanId, Map.of("http.method", "POST"));

                final List<Span> emittedSpans = processConcurrently(executorService, processor, rootSpan, childSpan);

                final List<String> emittedSpanIds = sortedSpanIds(emittedSpans);
                assertThat("iteration " + iteration, emittedSpanIds, equalTo(List.of(childSpanId, rootSpanId)));
                assertThat("iteration " + iteration, byId(emittedSpans, childSpanId).getTraceGroup(), equalTo(traceGroupName));
            }
        } finally {
            executorService.shutdownNow();
        }

        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void test_when_genAiRootAndChildAreProcessedConcurrentlyWithoutHold_then_onlyTheRootIsMarkedAsTheAiRoot() throws Exception {
        runGenAiRootAndChildConcurrently(Duration.ZERO);
    }

    @Test
    void test_when_genAiRootAndChildAreProcessedConcurrentlyWithTheDefaultHold_then_onlyTheRootIsMarkedAsTheAiRoot()
            throws Exception {
        runGenAiRootAndChildConcurrently(OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_HOLD);
    }

    private static void runGenAiRootAndChildConcurrently(final Duration genAiRootHold) throws Exception {
        final OTelTraceRawProcessor processor = createProcessor(genAiRootHold);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < GENAI_ITERATIONS; iteration++) {
                final String traceId = "genai-race-trace-" + iteration;
                final String traceGroupName = "genai-trace-group-" + iteration;
                final String rootSpanId = "root-" + iteration;
                final String childSpanId = "chat-" + iteration;
                final Span rootSpan = rootSpan(traceId, rootSpanId, traceGroupName,
                        Map.of("gen_ai.operation.name", "invoke_agent"));
                final Span childSpan = childSpan(traceId, childSpanId, rootSpanId, Map.of(
                        "gen_ai.operation.name", "chat",
                        "gen_ai.request.model", "gpt-4o",
                        "gen_ai.usage.input_tokens", 12L,
                        "gen_ai.usage.output_tokens", 5L));

                final List<Span> emittedSpans = processConcurrently(executorService, processor, rootSpan, childSpan);
                // Releases a root still held after both workers finished; normally none is.
                processor.doExecute(List.of()).forEach(record -> emittedSpans.add(record.getData()));

                final List<String> emittedSpanIds = sortedSpanIds(emittedSpans);
                assertThat("iteration " + iteration, emittedSpanIds, equalTo(List.of(childSpanId, rootSpanId)));
                final Span emittedChildSpan = byId(emittedSpans, childSpanId);
                assertThat("iteration " + iteration, emittedChildSpan.getTraceGroup(), equalTo(traceGroupName));
                assertThat("iteration " + iteration + " child marked",
                        emittedChildSpan.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), not(equalTo(true)));
                assertThat("iteration " + iteration + " root marked",
                        byId(emittedSpans, rootSpanId).getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
            }
        } finally {
            executorService.shutdownNow();
        }

        assertThat(processor.isReadyForShutdown(), is(true));
    }

    /** Lines up two workers on a latch, one processing the root and one the child, and collects both outputs. */
    private static List<Span> processConcurrently(final ExecutorService executorService, final OTelTraceRawProcessor processor,
                                                  final Span rootSpan, final Span childSpan) throws Exception {
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
        return emittedSpans;
    }

    private static OTelTraceRawProcessor createProcessor(final Duration genAiRootHold) {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        when(config.getGenAiRootHold()).thenReturn(genAiRootHold);
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), mock(PluginMetrics.class));
    }

    private static Span rootSpan(final String traceId, final String spanId, final String traceGroupName,
                                 final Map<String, Object> attributes) {
        return spanBuilder(traceId, spanId, "", attributes)
                .withTraceGroup(traceGroupName)
                .withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode(0)
                        .withEndTime("2026-01-01T00:01:00Z")
                        .withDurationInNanos(1L)
                        .build())
                .build();
    }

    private static Span childSpan(final String traceId, final String spanId, final String parentSpanId,
                                  final Map<String, Object> attributes) {
        return spanBuilder(traceId, spanId, parentSpanId, attributes)
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .build();
    }

    private static JacksonSpan.Builder spanBuilder(final String traceId, final String spanId, final String parentSpanId,
                                                   final Map<String, Object> attributes) {
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
                .withAttributes(new HashMap<>(attributes));
    }

    private static List<String> sortedSpanIds(final Collection<Span> spans) {
        return spans.stream().map(Span::getSpanId).sorted().collect(Collectors.toList());
    }

    private static Span byId(final Collection<Span> spans, final String spanId) {
        return spans.stream().filter(span -> spanId.equals(span.getSpanId())).findFirst().orElseThrow();
    }
}
