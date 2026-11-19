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
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.oteltrace.GenAiEnrichmentHelper.DERIVED_GEN_AI_IS_ROOT_KEY;

/**
 * A GenAI root span that is still missing a model or token usage is held up to genai_root_hold,
 * so children processed in later batches can complete it before it is written.
 */
class GenAiRootHoldTest {

    private static final String TRACE_ID = "ROOT_HOLD_TRACE";

    @Test
    void test_when_rootArrivesBeforeItsChildren_then_rootIsHeldAndEmittedEnrichedOnChildArrival() {
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), mock(PluginMetrics.class));

        final List<Span> firstBatch = execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));
        assertThat(spanIds(firstBatch), is(empty()));

        final List<Span> secondBatch = execute(processor, List.of(
                span("chat", "root", "2026-01-01T00:00:01Z", Map.of(
                        "gen_ai.operation.name", "chat",
                        "gen_ai.request.model", "gpt-4o",
                        "gen_ai.usage.input_tokens", 12L,
                        "gen_ai.usage.output_tokens", 5L))));

        assertThat(spanIds(secondBatch), containsInAnyOrder("root", "chat"));
        final Map<String, Object> root = byId(secondBatch, "root").getAttributes();
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(((Number) root.get("gen_ai.usage.input_tokens")).longValue(), equalTo(12L));
        assertThat(((Number) root.get("gen_ai.usage.output_tokens")).longValue(), equalTo(5L));
        assertThat(root.get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
    }

    @Test
    void test_when_childIsProcessedWhileItsRootIsHeld_then_childIsNotMarkedAsTheAiRoot() {
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), mock(PluginMetrics.class));

        execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));
        final List<Span> childBatch = execute(processor, List.of(
                span("chat", "root", "2026-01-01T00:00:01Z", Map.of(
                        "gen_ai.operation.name", "chat",
                        "gen_ai.request.model", "gpt-4o",
                        "gen_ai.usage.input_tokens", 12L))));

        assertThat(byId(childBatch, "chat").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), nullValue());
        assertThat(byId(childBatch, "root").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
    }

    @Test
    void test_when_noChildArrives_then_heldRootIsEmittedAtTheDeadlineMarkedButWithoutModelOrTokens() {
        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
        final OTelTraceRawProcessor processor = processor(Duration.ofMillis(200), pluginMetrics);

        final List<Span> firstBatch = execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));
        assertThat(spanIds(firstBatch), is(empty()));
        assertThat(gauge(pluginMetrics, OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(1.0));

        waitAtLeast(300);
        final List<Span> laterBatch = execute(processor, List.of());

        assertThat(spanIds(laterBatch), contains("root"));
        final Map<String, Object> root = byId(laterBatch, "root").getAttributes();
        assertThat(root.get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(root.get("gen_ai.request.model"), nullValue());
        assertThat(root.get("gen_ai.usage.input_tokens"), nullValue());
        assertThat(gauge(pluginMetrics, OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(0.0));
    }

    @Test
    void test_when_childrenAreWaitingInMemory_then_rootIsNotHeldAndIsEnrichedImmediately() {
        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), pluginMetrics);

        final List<Span> childFirstBatch = execute(processor, List.of(
                span("chat", "root", "2026-01-01T00:00:01Z", Map.of(
                        "gen_ai.operation.name", "chat",
                        "gen_ai.request.model", "gpt-4o",
                        "gen_ai.usage.input_tokens", 12L,
                        "gen_ai.usage.output_tokens", 5L))));
        assertThat(spanIds(childFirstBatch), is(empty()));

        final List<Span> rootBatch = execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));

        assertThat(spanIds(rootBatch), containsInAnyOrder("root", "chat"));
        final Map<String, Object> root = byId(rootBatch, "root").getAttributes();
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(((Number) root.get("gen_ai.usage.input_tokens")).longValue(), equalTo(12L));
        assertThat(gauge(pluginMetrics, OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(0.0));
    }

    @Test
    void test_when_anAgentAndItsCallReportUsageInDifferentBatches_then_rootGetsTheAgentsTotalOnly() {
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), mock(PluginMetrics.class));

        execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));
        final List<Span> agentBatch = execute(processor, List.of(
                span("agent", "root", "2026-01-01T00:00:01Z", Map.of(
                        "gen_ai.operation.name", "invoke_agent",
                        "gen_ai.usage.input_tokens", 15L,
                        "gen_ai.usage.output_tokens", 7L))));
        // The agent provided tokens but no model: the root stays held.
        assertThat(spanIds(agentBatch), contains("agent"));

        final List<Span> chatBatch = execute(processor, List.of(
                span("chat", "agent", "2026-01-01T00:00:02Z", Map.of(
                        "gen_ai.operation.name", "chat",
                        "gen_ai.request.model", "gpt-4o",
                        "gen_ai.usage.input_tokens", 10L,
                        "gen_ai.usage.output_tokens", 4L))));

        assertThat(spanIds(chatBatch), containsInAnyOrder("chat", "root"));
        final Map<String, Object> root = byId(chatBatch, "root").getAttributes();
        assertThat(root.get("gen_ai.request.model"), equalTo("gpt-4o"));
        assertThat(((Number) root.get("gen_ai.usage.input_tokens")).longValue(), equalTo(15L));
        assertThat(((Number) root.get("gen_ai.usage.output_tokens")).longValue(), equalTo(7L));
    }

    @Test
    void test_when_rootIsNotAGenAiSpan_then_rootIsNeverHeld() {
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), mock(PluginMetrics.class));

        final List<Span> batch = execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("http.method", "POST"))));

        assertThat(spanIds(batch), contains("root"));
    }

    @Test
    void test_when_rootAlreadyHasModelAndTokens_then_rootIsNotHeld() {
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), mock(PluginMetrics.class));

        final List<Span> batch = execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of(
                        "gen_ai.operation.name", "invoke_agent",
                        "gen_ai.request.model", "gpt-4o",
                        "gen_ai.usage.input_tokens", 12L))));

        assertThat(spanIds(batch), contains("root"));
    }

    @Test
    void test_when_genAiRootHoldIsZero_then_genAiRootsAreEmittedImmediately() {
        final OTelTraceRawProcessor processor = processor(Duration.ZERO, mock(PluginMetrics.class));

        final List<Span> batch = execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));

        assertThat(spanIds(batch), contains("root"));
        assertThat(byId(batch, "root").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
    }

    @Test
    void test_when_shuttingDown_then_heldRootsAreFlushedAndCountAgainstReadiness() {
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), mock(PluginMetrics.class));

        execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));
        assertThat(processor.isReadyForShutdown(), is(false));

        processor.prepareForShutdown();
        final List<Span> flushed = execute(processor, List.of());

        assertThat(spanIds(flushed), hasItem("root"));
        assertThat(byId(flushed, "root").getAttributes().get(DERIVED_GEN_AI_IS_ROOT_KEY), equalTo(true));
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void test_when_aLateChildStillCannotCompleteTheRoot_then_rootStaysHeld() {
        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
        final OTelTraceRawProcessor processor = processor(Duration.ofSeconds(30), pluginMetrics);

        execute(processor, List.of(
                span("root", "", "2026-01-01T00:00:00Z", Map.of("gen_ai.operation.name", "invoke_agent"))));
        final List<Span> childBatch = execute(processor, List.of(
                span("tool", "root", "2026-01-01T00:00:01Z", Map.of("gen_ai.operation.name", "execute_tool"))));

        assertThat(spanIds(childBatch), contains("tool"));
        assertThat(spanIds(childBatch), not(hasItem("root")));
        assertThat(gauge(pluginMetrics, OTelTraceRawProcessor.GENAI_HELD_ROOT_COUNT_METRIC_NAME), equalTo(1.0));
    }

    private static OTelTraceRawProcessor processor(final Duration rootHold, final PluginMetrics pluginMetrics) {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(180L);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.isGenAiEnrichmentDisabled()).thenReturn(false);
        when(config.getGenAiCoverageCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE);
        when(config.getGenAiRootPropagation()).thenReturn(GenAiRootPropagation.AI_ROOT);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        when(config.getGenAiRootHold()).thenReturn(rootHold);
        return new OTelTraceRawProcessor(config, mock(PipelineDescription.class), pluginMetrics);
    }

    private static List<Span> execute(final OTelTraceRawProcessor processor, final List<Span> spans) {
        return processor.doExecute(spans.stream().map(Record::new).collect(Collectors.toList())).stream()
                .map(Record::getData).collect(Collectors.toList());
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

    private static List<String> spanIds(final Collection<Span> spans) {
        return spans.stream().map(Span::getSpanId).collect(Collectors.toList());
    }

    private static Span byId(final Collection<Span> spans, final String spanId) {
        return spans.stream().filter(s -> spanId.equals(s.getSpanId())).findFirst().orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static double gauge(final PluginMetrics pluginMetrics, final String name) {
        final ArgumentCaptor<Object> measured = ArgumentCaptor.forClass(Object.class);
        final ArgumentCaptor<ToDoubleFunction> function = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(name), measured.capture(), function.capture());
        return function.getValue().applyAsDouble(measured.getValue());
    }

    private static void waitAtLeast(final long delayMillis) {
        final long startMillis = System.currentTimeMillis();
        await().atMost(delayMillis + 5_000, TimeUnit.MILLISECONDS)
                .until(() -> System.currentTimeMillis() - startMillis >= delayMillis);
    }
}
