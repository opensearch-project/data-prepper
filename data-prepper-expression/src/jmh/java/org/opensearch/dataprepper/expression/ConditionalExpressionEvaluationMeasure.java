/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Map;

public class ConditionalExpressionEvaluationMeasure {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private GenericExpressionEvaluator evaluator;
        private Event event;

        @Setup
        public void setUp() {
            final AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
            applicationContext.scan("org.opensearch.dataprepper.expression");
            applicationContext.refresh();

            evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

            final EventFactory eventFactory = TestEventFactory.getTestEventFactory();

            final Map<String, Object> eventData = Map.of("key", "a");

            event = eventFactory.eventBuilder(LogEventBuilder.class)
                    .withData(eventData)
                    .build();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 1)
    @Measurement(iterations = 5, time = 10)
    public Object evaluate_simple_equality_expression(final BenchmarkState benchmarkState) {
        final GenericExpressionEvaluator evaluator = benchmarkState.evaluator;
        return evaluator.evaluate("/key == \"a\"", benchmarkState.event);
    }
}
