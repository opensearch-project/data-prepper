/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.genai;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class OTelTraceGenAiProcessorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private OTelTraceGenAiProcessor processor;

    @BeforeEach
    void setup() {
        processor = new OTelTraceGenAiProcessor(mock(PluginMetrics.class));
    }

    @Test
    void testGenAiAttributesPropagatedFromChildToRoot() {
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-root-span.json")),
                new Record<>(buildSpanFromJsonFile("genai-child-span.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);
        final Span rootSpan = findRootSpan(result);

        assertThat(rootSpan.getAttributes().get("gen_ai.system"), equalTo("aws.bedrock"));
        assertThat(rootSpan.getAttributes().get("gen_ai.provider.name"), equalTo("aws.bedrock"));
        assertThat(rootSpan.getAttributes().get("gen_ai.agent.name"), equalTo("Weather Agent"));
        assertThat(rootSpan.getAttributes().get("gen_ai.request.model"), equalTo("anthropic.claude-sonnet-4.6"));
    }

    @Test
    void testGenAiSystemNotOverwrittenWhenRootAlreadyHasIt() {
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-root-span-with-genai-attr.json")),
                new Record<>(buildSpanFromJsonFile("genai-child-span-with-genai-root.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);
        final Span rootSpan = findRootSpan(result);

        assertThat(rootSpan.getAttributes().get("gen_ai.system"), equalTo("openai"));
    }

    @Test
    void testTokenCountsAggregatedFromChildrenToRoot() {
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-tokens-root-span.json")),
                new Record<>(buildSpanFromJsonFile("genai-tokens-child-span-1.json")),
                new Record<>(buildSpanFromJsonFile("genai-tokens-child-span-2.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);
        final Span rootSpan = findRootSpan(result);

        assertThat(((Number) rootSpan.getAttributes().get("gen_ai.usage.input_tokens")).longValue(), equalTo(300L));
        assertThat(((Number) rootSpan.getAttributes().get("gen_ai.usage.output_tokens")).longValue(), equalTo(125L));
    }

    @Test
    void testTokenCountsNotOverwrittenWhenRootAlreadyHasThem() {
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-tokens-root-span-with-tokens.json")),
                new Record<>(buildSpanFromJsonFile("genai-tokens-child-span-with-root-tokens.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);
        final Span rootSpan = findRootSpan(result);

        assertThat(((Number) rootSpan.getAttributes().get("gen_ai.usage.input_tokens")).longValue(), equalTo(500L));
        assertThat(((Number) rootSpan.getAttributes().get("gen_ai.usage.output_tokens")).longValue(), equalTo(200L));
    }

    @Test
    void testNoGenAiPropagationWhenNoChildHasGenAiAttrs() {
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-tokens-root-span.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);
        final Span rootSpan = findRootSpan(result);

        assertFalse(rootSpan.getAttributes().containsKey("gen_ai.system"));
    }

    @Test
    void testStripFlattenedSubkeysPreventsMappingConflict() {
        // Simulates CrewAI/OpenLLMetry spans with both parent string and flattened sub-keys
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-flattened-subkeys-root-span.json")),
                new Record<>(buildSpanFromJsonFile("genai-flattened-subkeys-child-span.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);

        final Span childSpan = result.stream()
                .map(Record::getData)
                .filter(s -> !s.getParentSpanId().isEmpty())
                .findFirst().orElseThrow();

        final Map<String, Object> attrs = childSpan.getAttributes();

        // Parent string values should be preserved
        assertThat(attrs.get("llm.input_messages"), equalTo("[{\"role\":\"user\",\"content\":\"What is 2+2?\"}]"));
        assertThat(attrs.get("llm.output_messages"), equalTo("[{\"role\":\"assistant\",\"content\":\"Four\"}]"));

        // Flattened sub-keys should be stripped (parent exists)
        assertFalse(attrs.containsKey("llm.input_messages.0.message.role"));
        assertFalse(attrs.containsKey("llm.input_messages.0.message.content"));
        assertFalse(attrs.containsKey("llm.output_messages.0.message.role"));
        assertFalse(attrs.containsKey("llm.output_messages.0.message.content"));

        // gen_ai.prompt sub-keys should be KEPT (no parent "gen_ai.prompt" string exists)
        assertThat(attrs.get("gen_ai.prompt.0.role"), equalTo("user"));
        assertThat(attrs.get("gen_ai.prompt.0.content"), equalTo("What is 2+2?"));
        assertThat(attrs.get("gen_ai.completion.0.role"), equalTo("assistant"));
        assertThat(attrs.get("gen_ai.completion.0.content"), equalTo("Four"));

        // gen_ai core attrs should still be there
        assertThat(attrs.get("gen_ai.system"), equalTo("AWS"));

        // Root should still get propagated attrs
        final Span rootSpan = findRootSpan(result);
        assertThat(rootSpan.getAttributes().get("gen_ai.system"), equalTo("AWS"));
    }

    @Test
    void testCrossTraceIsolation() {
        // GenAI trace + non-GenAI trace in same batch
        final List<Record<Span>> records = new ArrayList<>();
        records.add(new Record<>(buildSpanFromJsonFile("genai-tokens-root-span.json")));
        records.add(new Record<>(buildSpanFromJsonFile("genai-tokens-child-span-1.json")));
        records.add(new Record<>(buildSpanFromJsonFile("plain-root-span.json")));
        records.add(new Record<>(buildSpanFromJsonFile("plain-child-span.json")));

        processor.doExecute(records);

        final Span plainRoot = records.stream()
                .map(Record::getData)
                .filter(s -> "PLAIN_TRACE_ID".equals(s.getTraceId()) && s.getParentSpanId().isEmpty())
                .findFirst().orElseThrow();

        assertFalse(plainRoot.getAttributes().containsKey("gen_ai.system"));
        assertFalse(plainRoot.getAttributes().containsKey("gen_ai.usage.input_tokens"));
    }

    @Test
    void testBatchWithNoRootSpan() {
        // Only child spans, no root — should not throw
        final List<Record<Span>> records = Arrays.asList(
                new Record<>(buildSpanFromJsonFile("genai-child-span.json"))
        );

        final Collection<Record<Span>> result = processor.doExecute(records);
        Assertions.assertThat(result).hasSize(1);
    }

    @Test
    void testRootSpanWithZeroPaddedParentSpanId() {
        // Some instrumentation libraries emit "0000000000000000" instead of "" for root spans
        final Span rootSpan = buildSpanWithParentId("ZERO_TRACE", "ZERO_ROOT", "0000000000000000", "agent_invoke", Map.of());
        final Span childSpan = buildSpanWithParentId("ZERO_TRACE", "ZERO_CHILD", "ZERO_ROOT", "ChatBedrock.chat",
                Map.of("gen_ai.system", "aws.bedrock", "gen_ai.usage.input_tokens", 100, "gen_ai.usage.output_tokens", 50));

        final List<Record<Span>> records = Arrays.asList(new Record<>(rootSpan), new Record<>(childSpan));
        processor.doExecute(records);

        assertThat(rootSpan.getAttributes().get("gen_ai.system"), equalTo("aws.bedrock"));
        assertThat(((Number) rootSpan.getAttributes().get("gen_ai.usage.input_tokens")).longValue(), equalTo(100L));
    }

    @Test
    void testFirstChildWinsForGenAiSystem() {
        // Multi-model trace: first child has bedrock, second has openai — first wins
        final Span rootSpan = buildSpanWithParentId("MULTI_TRACE", "MULTI_ROOT", "", "agent_invoke", Map.of());
        final Span child1 = buildSpanWithParentId("MULTI_TRACE", "MULTI_CHILD_1", "MULTI_ROOT", "ChatBedrock.chat",
                Map.of("gen_ai.system", "aws.bedrock"));
        final Span child2 = buildSpanWithParentId("MULTI_TRACE", "MULTI_CHILD_2", "MULTI_ROOT", "ChatOpenAI.chat",
                Map.of("gen_ai.system", "openai"));

        final List<Record<Span>> records = Arrays.asList(new Record<>(rootSpan), new Record<>(child1), new Record<>(child2));
        processor.doExecute(records);

        assertThat(rootSpan.getAttributes().get("gen_ai.system"), equalTo("aws.bedrock"));
    }

    @Test
    void testProcessorContinuesAfterDeleteFailure() {
        // Span with null attributes should not cause NPE or halt processing
        final Span rootSpan = buildSpanWithParentId("SAFE_TRACE", "SAFE_ROOT", "", "agent_invoke", Map.of());
        final Span childSpan = buildSpanWithParentId("SAFE_TRACE", "SAFE_CHILD", "SAFE_ROOT", "ChatBedrock.chat",
                Map.of("gen_ai.system", "aws.bedrock"));

        final List<Record<Span>> records = Arrays.asList(new Record<>(rootSpan), new Record<>(childSpan));
        final Collection<Record<Span>> result = processor.doExecute(records);

        Assertions.assertThat(result).hasSize(2);
        assertThat(rootSpan.getAttributes().get("gen_ai.system"), equalTo("aws.bedrock"));
    }

    private Span findRootSpan(Collection<Record<Span>> records) {
        return records.stream()
                .map(Record::getData)
                .filter(s -> s.getParentSpanId() == null || s.getParentSpanId().isEmpty())
                .findFirst()
                .orElseThrow(() -> new AssertionError("No root span found"));
    }

    private static Span buildSpanFromJsonFile(final String jsonFileName) {
        JacksonSpan.Builder spanBuilder = JacksonSpan.builder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelTraceGenAiProcessorTest.class.getClassLoader().getResourceAsStream(jsonFileName))) {
            final Map<String, Object> spanMap = OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            spanBuilder = spanBuilder
                    .withTraceId((String) spanMap.get("traceId"))
                    .withSpanId((String) spanMap.get("spanId"))
                    .withParentSpanId((String) spanMap.get("parentSpanId"))
                    .withTraceState((String) spanMap.get("traceState"))
                    .withName((String) spanMap.get("name"))
                    .withKind((String) spanMap.get("kind"))
                    .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue())
                    .withStartTime((String) spanMap.get("startTime"))
                    .withEndTime((String) spanMap.get("endTime"))
                    .withTraceGroup(null);
            final Map<String, Object> attributes = (Map<String, Object>) spanMap.get("attributes");
            if (attributes != null) {
                spanBuilder = spanBuilder.withAttributes(attributes);
            }
            DefaultTraceGroupFields.Builder tgfBuilder = DefaultTraceGroupFields.builder();
            String parentSpanId = (String) spanMap.get("parentSpanId");
            if (parentSpanId == null || parentSpanId.isEmpty()) {
                Map<String, Object> tgf = (Map<String, Object>) spanMap.get("traceGroupFields");
                if (tgf != null) {
                    tgfBuilder.withStatusCode((Integer) tgf.get("statusCode"))
                            .withEndTime((String) tgf.get("endTime"))
                            .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue());
                }
                spanBuilder.withTraceGroup((String) spanMap.get("traceGroup"));
            }
            spanBuilder.withTraceGroupFields(tgfBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return spanBuilder.build();
    }

    private static Span buildSpanWithParentId(String traceId, String spanId, String parentSpanId, String name,
                                               Map<String, Object> attributes) {
        JacksonSpan.Builder builder = JacksonSpan.builder()
                .withTraceId(traceId)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withName(name)
                .withTraceGroup(name)
                .withKind("SPAN_KIND_CLIENT")
                .withStartTime("2026-02-17T12:00:00.000000000Z")
                .withEndTime("2026-02-17T12:00:01.000000000Z")
                .withDurationInNanos(1000000000L)
                .withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode(1)
                        .withEndTime("2026-02-17T12:00:01.000000000Z")
                        .withDurationInNanos(1000000000L)
                        .build())
                .withAttributes(new java.util.HashMap<>(attributes));
        return builder.build();
    }
}
