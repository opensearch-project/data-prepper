/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(2)
@Threads(4)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 5, time = 10)
public class DissectProcessorBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        Dissector dissector;
        String input;

        @Setup
        public void setUp() {
            dissector = new Dissector("%{timestamp} %{level} %{message}");
            input = "2024-01-15 ERROR service crashed";
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public Object benchmark_dissect(BenchmarkState state) {
        return state.dissector.dissectText(state.input);
    }
}