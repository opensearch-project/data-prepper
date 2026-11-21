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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * {@code genai_root_marking}: {@code agent_spans} marks every agent and workflow span from the span
 * alone, {@code none} marks nothing, and {@code outermost} (the default) is unchanged.
 */
class GenAiRootMarkingTest {

    private static final String TRACE_ID = "ROOT_MARKING_TRACE";

    @Test
    void agentSpans_marksEveryAgentAndWorkflowSpanAndNothingElse() {
        final List<Span> spans = nestedTrace();

        enrich(spans, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.AGENT_SPANS);

        assertEquals(List.of("planner", "weather", "workflow"), markedSpanIds(spans));
    }

    @ParameterizedTest
    @ValueSource(strings = {"chat", "execute_tool", "create_agent", "embeddings", "text_completion",
            "generate_content", "invoke_agents", "Invoke_Agent"})
    void agentSpans_neverMarksOtherOperations(final String operation) {
        final List<Span> spans = List.of(span("only", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", operation)));

        enrich(spans, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.AGENT_SPANS);

        assertEquals(List.of(), markedSpanIds(spans));
    }

    @Test
    void agentSpans_marksAVendorAgentSpanByItsNormalizedOperation() {
        final List<Span> spans = List.of(span("agent", "", "2026-01-01T00:00:00Z", Map.of("openinference.span.kind", "AGENT")));

        enrich(spans, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.AGENT_SPANS);

        assertEquals("invoke_agent", spans.get(0).getAttributes().get("gen_ai.operation.name"));
        assertEquals(List.of("agent"), markedSpanIds(spans));
    }

    @ParameterizedTest
    @MethodSource("batchSplits")
    void agentSpans_marksTheSameSpansWhateverTheBatchSplit(final String name, final List<List<String>> batches) {
        final Map<String, Span> byId = nestedTrace().stream().collect(Collectors.toMap(Span::getSpanId, s -> s));

        for (final List<String> batch : batches) {
            enrich(batch.stream().map(byId::get).collect(Collectors.toList()),
                    GenAiRootPropagation.AI_ROOT, GenAiRootMarking.AGENT_SPANS);
        }

        assertEquals(List.of("planner", "weather", "workflow"), markedSpanIds(byId.values()), name);
    }

    static Stream<Object[]> batchSplits() {
        final List<String> ids = nestedTrace().stream().map(Span::getSpanId).collect(Collectors.toList());
        final List<String> reversed = new ArrayList<>(ids);
        Collections.reverse(reversed);
        final Stream<Object[]> fixed = Stream.of(
                new Object[]{"one batch", List.of(ids)},
                new Object[]{"one span per batch, in order", ids.stream().map(List::of).collect(Collectors.toList())},
                new Object[]{"one span per batch, reversed", reversed.stream().map(List::of).collect(Collectors.toList())},
                new Object[]{"root last", List.of(ids.subList(1, ids.size()), ids.subList(0, 1))});
        final Random random = new Random(7);
        final Stream<Object[]> randomSplits = IntStream.range(0, 20).mapToObj(i -> {
            final List<String> shuffled = new ArrayList<>(ids);
            Collections.shuffle(shuffled, random);
            final List<List<String>> batches = new ArrayList<>();
            int start = 0;
            while (start < shuffled.size()) {
                final int end = Math.min(shuffled.size(), start + 1 + random.nextInt(3));
                batches.add(shuffled.subList(start, end));
                start = end;
            }
            return new Object[]{"random split " + i, batches};
        });
        return Stream.concat(fixed, randomSplits);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false", "TRUE", "yes"})
    void agentSpans_neverChangesASourceMarkerAndStillMarksTheOtherAgents(final String marker) {
        final List<Span> spans = nestedTrace();
        final Object value = "true".equals(marker) ? (Object) Boolean.TRUE : "false".equals(marker) ? Boolean.FALSE : marker;
        byId(spans, "planner").put("attributes/" + DERIVED_GEN_AI_IS_ROOT_KEY, value);

        enrich(spans, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.AGENT_SPANS);

        assertEquals(value, byId(spans, "planner").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY));
        assertEquals(Boolean.TRUE, byId(spans, "weather").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY));
        assertEquals(Boolean.TRUE, byId(spans, "workflow").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY));
    }

    @Test
    void agentSpans_givesTheSubtreeTokensOnlyToTheOutermostMarkedAgent() {
        final List<Span> spans = nestedTrace();

        enrich(spans, GenAiRootPropagation.NONE, GenAiRootMarking.AGENT_SPANS);

        // workflow > planner > (chat 10/4, weather > chat 5/3): only the workflow carries the total.
        assertTokens(byId(spans, "workflow"), 15L, 7L);
        assertNull(byId(spans, "planner").getAttributes().get("gen_ai.usage.input_tokens"));
        assertNull(byId(spans, "weather").getAttributes().get("gen_ai.usage.input_tokens"));
        final long markedInput = spans.stream()
                .filter(s -> Boolean.TRUE.equals(s.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY)))
                .mapToLong(s -> ((Number) s.getAttributes().getOrDefault("gen_ai.usage.input_tokens", 0L)).longValue())
                .sum();
        assertEquals(15L, markedInput);
    }

    @Test
    void agentSpans_givesANestedAgentItsTokensWhenItsParentAgentIsInAnotherBatch() {
        final Map<String, Span> byId = nestedTrace().stream().collect(Collectors.toMap(Span::getSpanId, s -> s));

        enrich(List.of(byId.get("weather"), byId.get("chat2")), GenAiRootPropagation.NONE, GenAiRootMarking.AGENT_SPANS);

        assertTokens(byId.get("weather"), 5L, 3L);
    }

    @Test
    void agentSpans_keepsTheTokensAnAgentReports() {
        final List<Span> spans = nestedTrace();
        byId(spans, "workflow").put("attributes/gen_ai.usage.input_tokens", 99L);

        enrich(spans, GenAiRootPropagation.NONE, GenAiRootMarking.AGENT_SPANS);

        assertEquals(99L, ((Number) byId(spans, "workflow").getAttributes().get("gen_ai.usage.input_tokens")).longValue());
        assertNull(byId(spans, "planner").getAttributes().get("gen_ai.usage.input_tokens"));
    }

    @Test
    void none_marksNothing() {
        final List<Span> spans = nestedTrace();

        enrich(spans, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.NONE);

        assertEquals(List.of(), markedSpanIds(spans));
    }

    @Test
    void none_keepsASourceMarkerAndPropagatesToTheMarkedRoot() {
        final List<Span> spans = nestedTrace();
        byId(spans, "http").put("attributes/" + DERIVED_GEN_AI_IS_ROOT_KEY, true);

        enrich(spans, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.NONE);

        assertEquals(List.of("http"), markedSpanIds(spans));
        assertEquals("Travel Planner", byId(spans, "http").getAttributes().get("gen_ai.agent.name"));
        assertTokens(byId(spans, "http"), 15L, 7L);
    }

    @Test
    void none_stillPropagatesToAGenAiRoot() {
        final List<Span> spans = nestedTrace().stream().filter(s -> !"http".equals(s.getSpanId())).collect(Collectors.toList());
        final List<Span> rooted = new ArrayList<>();
        for (final Span s : spans) {
            rooted.add("workflow".equals(s.getSpanId())
                    ? span("workflow", "", "2026-01-01T00:00:01Z", Map.of("gen_ai.operation.name", "invoke_workflow"))
                    : s);
        }

        enrich(rooted, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.NONE);

        assertEquals(List.of(), markedSpanIds(rooted));
        assertTokens(byId(rooted, "workflow"), 15L, 7L);
    }

    @Test
    void outermost_isTheDefaultAndMarksOnlyTheOutermostGenAiSpan() {
        final List<Span> withDefault = nestedTrace();
        final List<Span> withOutermost = nestedTrace();

        GenAiEnrichmentHelper.enrichBatch(withDefault, GenAiCoverage.NONE, GenAiRootPropagation.AI_ROOT);
        enrich(withOutermost, GenAiRootPropagation.AI_ROOT, GenAiRootMarking.OUTERMOST);

        assertEquals(List.of("workflow"), markedSpanIds(withDefault));
        assertEquals(attributesById(withDefault), attributesById(withOutermost));
    }

    @ParameterizedTest
    @EnumSource(GenAiRootMarking.class)
    void processor_marksANestedAgentThatArrivesAfterItsRootByTheConfiguredMode(final GenAiRootMarking marking) {
        final OTelTraceRawProcessor processor = processor(marking);
        final List<Span> trace = nestedTrace();
        final List<Span> first = trace.stream().filter(s -> !"weather".equals(s.getSpanId()) && !"chat2".equals(s.getSpanId()))
                .collect(Collectors.toList());
        final List<Span> late = List.of(byId(trace, "weather"), byId(trace, "chat2"));

        final List<Span> written = new ArrayList<>(execute(processor, first));
        written.addAll(execute(processor, late));

        final Map<GenAiRootMarking, List<String>> expected = Map.of(
                GenAiRootMarking.OUTERMOST, List.of("workflow"),
                GenAiRootMarking.AGENT_SPANS, List.of("planner", "weather", "workflow"),
                GenAiRootMarking.NONE, List.of());
        assertEquals(expected.get(marking), markedSpanIds(written));
    }

    private static void enrich(final List<Span> spans, final GenAiRootPropagation propagation, final GenAiRootMarking marking) {
        GenAiEnrichmentHelper.enrichBatch(spans, GenAiCoverage.NONE, propagation, marking);
    }

    /** {@code POST /plan} > workflow > planner > (chat1, tool > weather > chat2). */
    private static List<Span> nestedTrace() {
        final List<Span> spans = new ArrayList<>();
        spans.add(span("http", "", "2026-01-01T00:00:00Z", Map.of("http.method", "POST")));
        spans.add(span("workflow", "http", "2026-01-01T00:00:01Z", Map.of("gen_ai.operation.name", "invoke_workflow")));
        spans.add(span("planner", "workflow", "2026-01-01T00:00:02Z", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.agent.name", "Travel Planner")));
        spans.add(span("chat1", "planner", "2026-01-01T00:00:03Z", Map.of(
                "gen_ai.operation.name", "chat", "gen_ai.usage.input_tokens", 10L, "gen_ai.usage.output_tokens", 4L)));
        spans.add(span("tool", "planner", "2026-01-01T00:00:04Z", Map.of("gen_ai.operation.name", "execute_tool")));
        spans.add(span("weather", "tool", "2026-01-01T00:00:05Z", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.agent.name", "Weather Assistant")));
        spans.add(span("chat2", "weather", "2026-01-01T00:00:06Z", Map.of(
                "gen_ai.operation.name", "chat", "gen_ai.usage.input_tokens", 5L, "gen_ai.usage.output_tokens", 3L)));
        return spans;
    }

    private static OTelTraceRawProcessor processor(final GenAiRootMarking marking) {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(180L);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getGenAiRootMarking()).thenReturn(marking);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), mock(PluginMetrics.class));
    }

    private static List<Span> execute(final OTelTraceRawProcessor processor, final List<Span> spans) {
        return processor.doExecute(spans.stream().map(Record::new).collect(Collectors.toList()))
                .stream().map(Record::getData).collect(Collectors.toList());
    }

    private static Span span(final String spanId, final String parentSpanId, final String startTime,
                             final Map<String, Object> attributes) {
        return JacksonSpan.builder()
                .withTraceId(TRACE_ID)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withTraceState("")
                .withName(spanId)
                .withKind("INTERNAL")
                .withDurationInNanos(1L)
                .withStartTime(startTime)
                .withEndTime("2026-01-01T00:01:00Z")
                .withTraceGroup(null)
                .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                .withAttributes(new HashMap<>(attributes))
                .build();
    }

    private static void assertTokens(final Span span, final long input, final long output) {
        assertEquals(input, ((Number) span.getAttributes().get("gen_ai.usage.input_tokens")).longValue(), span.getSpanId());
        assertEquals(output, ((Number) span.getAttributes().get("gen_ai.usage.output_tokens")).longValue(), span.getSpanId());
    }

    private static Map<String, Map<String, Object>> attributesById(final Collection<Span> spans) {
        return spans.stream().collect(Collectors.toMap(Span::getSpanId, Span::getAttributes));
    }

    private static List<String> markedSpanIds(final Collection<Span> spans) {
        return spans.stream()
                .filter(s -> Boolean.TRUE.equals(s.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY)))
                .map(Span::getSpanId)
                .sorted()
                .collect(Collectors.toList());
    }

    private static Span byId(final Collection<Span> spans, final String spanId) {
        return spans.stream().filter(s -> spanId.equals(s.getSpanId())).findFirst().orElseThrow();
    }
}
