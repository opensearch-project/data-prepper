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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * Whatever order the spans of a trace arrive in, one per batch, exactly one span is marked as the
 * trace's AI root and every span is written exactly once, with nothing left waiting once every span
 * has arrived.
 */
class GenAiOutOfOrderMarkingTest {

    private static final String TRACE_ID = "OUT_OF_ORDER_TRACE";

    /**
     * invoke_agent R &gt; invoke_agent S &gt; chat C. In the order S, R, C the root is written with S,
     * which carries no model or tokens, before C arrives: drained children suppress the root hold
     * whatever data they carry, so R's enrichment is only checked in the other orders.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("agentRootTraceOrders")
    void test_when_anAgentRootTraceArrivesInAnyOrder_then_onlyTheRootIsMarked(final List<String> order) {
        final List<Span> emittedSpans = processOneSpanPerBatch(order, agentRootTrace());

        assertThat(sortedSpanIds(emittedSpans), equalTo(List.of("C", "R", "S")));
        assertThat(markedSpanIds(emittedSpans), equalTo(List.of("R")));
        if (!order.equals(List.of("S", "R", "C"))) {
            final Map<String, Object> root = byId(emittedSpans, "R").getAttributes();
            assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
            assertThat(((Number) root.get("gen_ai.usage.input_tokens")).longValue(), equalTo(150L));
            assertThat(((Number) root.get("gen_ai.usage.output_tokens")).longValue(), equalTo(85L));
        }
    }

    /** POST /chat H &gt; invoke_agent A &gt; invoke_agent S &gt; chat C. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("httpRootTraceOrders")
    void test_when_anHttpRootTraceArrivesInAnyOrder_then_onlyTheOutermostAgentIsMarked(final List<String> order) {
        final List<Span> emittedSpans = processOneSpanPerBatch(order, httpRootTrace());

        assertThat(sortedSpanIds(emittedSpans), equalTo(List.of("A", "C", "H", "S")));
        assertThat(markedSpanIds(emittedSpans), equalTo(List.of("A")));
    }

    private static Stream<List<String>> agentRootTraceOrders() {
        return permutations(List.of("R", "S", "C")).stream();
    }

    private static Stream<List<String>> httpRootTraceOrders() {
        return permutations(List.of("H", "A", "S", "C")).stream();
    }

    /** Feeds each span in its own batch, then an empty batch to release anything still held. */
    private static List<Span> processOneSpanPerBatch(final List<String> order, final Map<String, Span> spansById) {
        final OTelTraceRawProcessor processor = createProcessor();
        final List<Span> emittedSpans = new ArrayList<>();
        for (final String spanId : order) {
            processor.doExecute(List.of(new Record<>(spansById.get(spanId))))
                    .forEach(record -> emittedSpans.add(record.getData()));
        }
        processor.doExecute(List.of()).forEach(record -> emittedSpans.add(record.getData()));
        assertThat("nothing waits for a deadline", processor.isReadyForShutdown(), is(true));
        return emittedSpans;
    }

    private static OTelTraceRawProcessor createProcessor() {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        when(config.getGenAiRootHold()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_ROOT_HOLD);
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), mock(PluginMetrics.class));
    }

    private static Map<String, Span> agentRootTrace() {
        return byId(List.of(
                rootSpan("R", Map.of("gen_ai.operation.name", "invoke_agent")),
                span("S", "R", Map.of("gen_ai.operation.name", "invoke_agent")),
                span("C", "S", chatAttributes())));
    }

    private static Map<String, Span> httpRootTrace() {
        return byId(List.of(
                rootSpan("H", Map.of("http.method", "POST")),
                span("A", "H", Map.of("gen_ai.operation.name", "invoke_agent")),
                span("S", "A", Map.of("gen_ai.operation.name", "invoke_agent")),
                span("C", "S", chatAttributes())));
    }

    private static Map<String, Object> chatAttributes() {
        return Map.of(
                "gen_ai.operation.name", "chat",
                "gen_ai.request.model", "gpt-4o",
                "gen_ai.usage.input_tokens", 150L,
                "gen_ai.usage.output_tokens", 85L);
    }

    private static Span rootSpan(final String spanId, final Map<String, Object> attributes) {
        return spanBuilder(spanId, "", attributes)
                .withTraceGroup("trace-group")
                .withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode(0)
                        .withEndTime("2026-01-01T00:01:00Z")
                        .withDurationInNanos(1L)
                        .build())
                .build();
    }

    private static Span span(final String spanId, final String parentSpanId, final Map<String, Object> attributes) {
        return spanBuilder(spanId, parentSpanId, attributes)
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .build();
    }

    private static JacksonSpan.Builder spanBuilder(final String spanId, final String parentSpanId,
                                                   final Map<String, Object> attributes) {
        return JacksonSpan.builder()
                .withTraceId(TRACE_ID)
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

    private static List<List<String>> permutations(final List<String> items) {
        if (items.isEmpty()) {
            return List.of(List.of());
        }
        final List<List<String>> permutations = new ArrayList<>();
        for (final String first : items) {
            final List<String> rest = new ArrayList<>(items);
            rest.remove(first);
            for (final List<String> tail : permutations(rest)) {
                final List<String> permutation = new ArrayList<>();
                permutation.add(first);
                permutation.addAll(tail);
                permutations.add(permutation);
            }
        }
        return permutations;
    }

    private static Map<String, Span> byId(final List<Span> spans) {
        return spans.stream().collect(Collectors.toMap(Span::getSpanId, Function.identity()));
    }

    private static Span byId(final List<Span> spans, final String spanId) {
        return spans.stream().filter(span -> spanId.equals(span.getSpanId())).findFirst().orElseThrow();
    }

    private static List<String> sortedSpanIds(final List<Span> spans) {
        return spans.stream().map(Span::getSpanId).sorted().collect(Collectors.toList());
    }

    private static List<String> markedSpanIds(final List<Span> spans) {
        return spans.stream()
                .filter(span -> Boolean.TRUE.equals(span.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY)))
                .map(Span::getSpanId)
                .sorted()
                .collect(Collectors.toList());
    }
}
