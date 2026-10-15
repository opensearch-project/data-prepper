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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * An agent behind an HTTP handler: the trace root is a non-GenAI span, and the outermost
 * GenAI span is marked as the trace's AI root.
 */
class GenAiAgentRootTest {

    private static final String TRACE_ID = "AGENT_ROOT_TRACE";

    @Test
    void marksTheOutermostGenAiSpanUnderAnHttpRoot() {
        final List<Span> spans = httpAgentTrace();
        Collections.shuffle(spans);

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals(List.of("agent"), markedSpanIds(spans));
        final Span http = byId(spans, "http");
        assertNull(http.getAttributes().get("gen_ai.operation.name"));
        assertNull(http.getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY));
    }

    @Test
    void aggregatesDescendantTokensOntoTheAgentRootWithoutDoubleCountingTheTraceRoot() {
        final List<Span> spans = httpAgentTrace();

        GenAiEnrichmentHelper.enrichBatch(spans);

        final Map<String, Object> agent = byId(spans, "agent").getAttributes();
        assertEquals(15L, ((Number) agent.get("gen_ai.usage.input_tokens")).longValue());
        assertEquals(7L, ((Number) agent.get("gen_ai.usage.output_tokens")).longValue());
        final Map<String, Object> http = byId(spans, "http").getAttributes();
        assertEquals(15L, ((Number) http.get("gen_ai.usage.input_tokens")).longValue());
        assertEquals(7L, ((Number) http.get("gen_ai.usage.output_tokens")).longValue());
    }

    @Test
    void propagatesTheOuterAgentNameToTheTraceRootWhateverTheBatchOrder() {
        final List<Span> spans = httpAgentTrace();
        spans.add(span("inner", "chat1", "2026-01-01T00:00:03Z", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.agent.name", "Weather Agent")));
        // Inner agent first in the batch: first-child-wins would pick it.
        Collections.reverse(spans);

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals("Travel Planner", byId(spans, "http").getAttributes().get("gen_ai.agent.name"));
    }

    @Test
    void leavesAGenAiRootUnmarked() {
        final List<Span> spans = new ArrayList<>();
        spans.add(span("agent", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent")));
        spans.add(span("chat1", "agent", "2026-01-01T00:00:01Z", Map.of("gen_ai.operation.name", "chat")));

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals(List.of(), markedSpanIds(spans));
    }

    @Test
    void marksEachParallelTopLevelAgent() {
        final List<Span> spans = new ArrayList<>();
        spans.add(span("http", "", "2026-01-01T00:00:00Z", Map.of("http.method", "POST")));
        spans.add(span("agentA", "http", "2026-01-01T00:00:01Z", Map.of("gen_ai.operation.name", "invoke_agent")));
        spans.add(span("agentB", "http", "2026-01-01T00:00:02Z", Map.of("gen_ai.operation.name", "invoke_agent")));
        spans.add(span("chatA", "agentA", "2026-01-01T00:00:03Z", Map.of("gen_ai.operation.name", "chat")));

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals(List.of("agentA", "agentB"), markedSpanIds(spans));
    }

    @Test
    void marksTheOutermostSpanInAnOrphanBatchWithoutTheRoot() {
        // Children flushed after trace_flush_interval, before the root arrived.
        final List<Span> spans = httpAgentTrace().stream()
                .filter(s -> !"http".equals(s.getSpanId()))
                .collect(Collectors.toCollection(ArrayList::new));

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals(List.of("agent"), markedSpanIds(spans));
    }

    @Test
    void keepsAMarkerSetAtTheSource() {
        final List<Span> spans = httpAgentTrace();
        // The SDK marked the HTTP handler span as the agent root.
        spans.set(0, span("http", "", "2026-01-01T00:00:00Z", Map.of(DERIVED_GEN_AI_IS_ROOT_KEY, true)));

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals(List.of("http"), markedSpanIds(spans));
    }

    @Test
    void marksNothingWithoutGenAiSpans() {
        final List<Span> spans = new ArrayList<>();
        spans.add(span("http", "", "2026-01-01T00:00:00Z", Map.of("http.method", "GET")));
        spans.add(span("db", "http", "2026-01-01T00:00:01Z", Map.of("db.system", "postgresql")));

        GenAiEnrichmentHelper.enrichBatch(spans);

        assertEquals(List.of(), markedSpanIds(spans));
    }

    @Test
    void marksTheAgentSpanThroughTheProcessorWhenTheRootArrivesLast() {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(180L);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL);
        final OTelTraceRawProcessor processor =
                new OTelTraceRawProcessor(config, mock(PipelineDescription.class), mock(PluginMetrics.class));

        final List<Span> spans = httpAgentTrace();
        final Span root = spans.remove(0);
        // Children are exported first and held until their root arrives.
        assertEquals(0, processor.doExecute(records(spans)).size());
        final Collection<Record<Span>> flushed = processor.doExecute(records(List.of(root)));

        final List<Span> out = flushed.stream().map(Record::getData).collect(Collectors.toList());
        assertEquals(List.of("agent"), markedSpanIds(out));
        assertFalse(byId(out, "http").getAttributes().containsKey(DERIVED_GEN_AI_IS_ROOT_KEY));
    }

    /** POST /runs > invoke_agent > chat, execute_tool > chat. */
    private static List<Span> httpAgentTrace() {
        final List<Span> spans = new ArrayList<>();
        spans.add(span("http", "", "2026-01-01T00:00:00Z", Map.of("http.method", "POST")));
        spans.add(span("agent", "http", "2026-01-01T00:00:01Z", Map.of(
                "gen_ai.operation.name", "invoke_agent", "gen_ai.agent.name", "Travel Planner")));
        spans.add(span("chat1", "agent", "2026-01-01T00:00:02Z", Map.of(
                "gen_ai.operation.name", "chat",
                "gen_ai.usage.input_tokens", 10L, "gen_ai.usage.output_tokens", 4L)));
        spans.add(span("tool", "agent", "2026-01-01T00:00:04Z", Map.of(
                "gen_ai.operation.name", "execute_tool")));
        spans.add(span("chat2", "tool", "2026-01-01T00:00:05Z", Map.of(
                "gen_ai.operation.name", "chat",
                "gen_ai.usage.input_tokens", 5L, "gen_ai.usage.output_tokens", 3L)));
        return spans;
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

    private static List<Record<Span>> records(final List<Span> spans) {
        return spans.stream().map(Record::new).collect(Collectors.toList());
    }
}
