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
import org.junit.jupiter.params.provider.Arguments;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * Whatever order the spans of an agent-root trace (invoke_agent R &gt; invoke_agent S &gt; chat C)
 * arrive in, one per batch, every span is written once and the marked spans are the ones the
 * marking mode selects: only the root with {@code outermost}, every agent span with
 * {@code agent_spans}, none with {@code none}.
 */
class GenAiOutOfOrderMarkingTest {

    private static final String TRACE_ID = "OUT_OF_ORDER_TRACE";

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("markingsAndOrders")
    void test_when_anAgentRootTraceArrivesInAnyOrder_then_theModeSelectsTheMarkedSpans(final GenAiRootMarking marking,
                                                                                       final List<String> order) {
        final OTelTraceRawProcessor processor = createProcessor(marking);
        final Map<String, Span> spansById = agentRootTrace();

        final List<Span> emittedSpans = new ArrayList<>();
        for (final String spanId : order) {
            processor.doExecute(List.of(new Record<>(spansById.get(spanId))))
                    .forEach(record -> emittedSpans.add(record.getData()));
        }

        final Map<GenAiRootMarking, List<String>> expectedMarked = Map.of(
                GenAiRootMarking.OUTERMOST, List.of("R"),
                GenAiRootMarking.AGENT_SPANS, List.of("R", "S"),
                GenAiRootMarking.NONE, List.of());
        assertThat(emittedSpans.stream().map(Span::getSpanId).sorted().collect(Collectors.toList()), equalTo(List.of("C", "R", "S")));
        assertThat(markedSpanIds(emittedSpans), equalTo(expectedMarked.get(marking)));
    }

    private static Stream<Arguments> markingsAndOrders() {
        final List<Arguments> arguments = new ArrayList<>();
        for (final GenAiRootMarking marking : GenAiRootMarking.values()) {
            for (final List<String> order : permutations(List.of("R", "S", "C"))) {
                arguments.add(Arguments.of(marking, order));
            }
        }
        return arguments.stream();
    }

    private static OTelTraceRawProcessor createProcessor(final GenAiRootMarking marking) {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getGenAiRootMarking()).thenReturn(marking);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), mock(PluginMetrics.class));
    }

    private static Map<String, Span> agentRootTrace() {
        return Stream.of(
                        span("R", "", "trace-group", Map.of("gen_ai.operation.name", "invoke_agent")),
                        span("S", "R", null, Map.of("gen_ai.operation.name", "invoke_agent")),
                        span("C", "S", null, Map.of(
                                "gen_ai.operation.name", "chat",
                                "gen_ai.request.model", "gpt-4o",
                                "gen_ai.usage.input_tokens", 150L,
                                "gen_ai.usage.output_tokens", 85L)))
                .collect(Collectors.toMap(Span::getSpanId, Function.identity()));
    }

    private static Span span(final String spanId, final String parentSpanId, final String traceGroup,
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
                .withTraceGroup(traceGroup)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .withAttributes(new HashMap<>(attributes))
                .build();
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

    private static List<String> markedSpanIds(final List<Span> spans) {
        return spans.stream()
                .filter(span -> Boolean.TRUE.equals(span.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY)))
                .map(Span::getSpanId)
                .sorted()
                .collect(Collectors.toList());
    }
}
