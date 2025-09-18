/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 5)
public class TranslateProcessorBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private TranslateProcessor staticKeyProcessor;
        private TranslateProcessor dynamicKeyProcessor;
        private TranslateProcessor patternProcessor;
        private TranslateProcessor nestedPathProcessor;
        private Collection<Record<Event>> staticKeyRecords;
        private Collection<Record<Event>> dynamicKeyRecords;
        private Collection<Record<Event>> patternRecords;
        private Collection<Record<Event>> nestedPathRecords;
        private ObjectMapper objectMapper;

        @Setup
        public void setUp() throws IOException {
            objectMapper = new ObjectMapper();
            setupProcessors();
            setupTestData();
        }

        private void setupProcessors() throws IOException {
            final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
            final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);

            staticKeyProcessor = createProcessor("static_key_config.json", pluginMetrics, expressionEvaluator);
            dynamicKeyProcessor = createProcessor("dynamic_key_config.json", pluginMetrics, expressionEvaluator);
            patternProcessor = createProcessor("pattern_config.json", pluginMetrics, expressionEvaluator);
            nestedPathProcessor = createProcessor("nested_path_config.json", pluginMetrics, expressionEvaluator);
        }

        private void setupTestData() throws IOException {
            staticKeyRecords = loadTestData("static_key_test_data.json");
            dynamicKeyRecords = loadTestData("dynamic_key_test_data.json");
            patternRecords = loadTestData("pattern_test_data.json");
            nestedPathRecords = loadTestData("nested_path_test_data.json");
        }

        private Collection<Record<Event>> loadTestData(String filename) throws IOException {
            try (InputStream is = getClass().getResourceAsStream("/jmh/" + filename)) {
                ObjectNode node = (ObjectNode) objectMapper.readTree(is);
                List<Map<String, Object>> records = objectMapper.convertValue(
                        node.get("testRecords"),
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Map.class)
                );
                return records.stream()
                        .map(data -> new Record<>(createEvent(data)))
                        .collect(Collectors.toList());
            }
        }

        private Event createEvent(final Map<String, Object> data) {
            return JacksonEvent.builder()
                    .withEventType("event")
                    .withData(data)
                    .build();
        }

        private TranslateProcessor createProcessor(
                String configFile,
                PluginMetrics pluginMetrics,
                ExpressionEvaluator expressionEvaluator
        ) throws IOException {
            try (InputStream is = getClass().getResourceAsStream("/jmh/" + configFile)) {
                TranslateProcessorConfig config = objectMapper.readValue(is, TranslateProcessorConfig.class);
                return new TranslateProcessor(pluginMetrics, config, expressionEvaluator);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark_static_key_translation(BenchmarkState state) {
        state.staticKeyProcessor.doExecute(state.staticKeyRecords);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark_dynamic_key_translation(BenchmarkState state) {
        state.dynamicKeyProcessor.doExecute(state.dynamicKeyRecords);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark_pattern_matching(BenchmarkState state) {
        state.patternProcessor.doExecute(state.patternRecords);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark_nested_path_translation(BenchmarkState state) {
        state.nestedPathProcessor.doExecute(state.nestedPathRecords);
    }
}