/*
 *  SPDX-License-Identifier: Apache-2.0
 *  
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.benchmarks.prepper.state;

import com.amazon.dataprepper.plugins.prepper.state.MapDbPrepperState;
import com.google.common.primitives.SignedBytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

@State(Scope.Benchmark)
public class MapDbPrepperStateBenchmarks {
    private static final int BATCH_SIZE = 100;
    private static final int NUM_BATCHES = 10000;
    private static final int DEFAULT_CONCURRENCY = 16;
    private static final Random RANDOM = new Random();
    private static final String DB_PATH = "data/benchmark";
    private static final String DB_NAME = "benchmarkDb";

    private MapDbPrepperState<String> mapDbPrepperState;
    private List<Map<byte[], String>> data = new ArrayList<Map<byte[], String>>(){{
        for(int i=0; i<NUM_BATCHES; i++) {
            final TreeMap<byte[], String> batch = new TreeMap<>(SignedBytes.lexicographicalComparator());
            for(int j=0; j<BATCH_SIZE; j++) {
                batch.put(getRandomBytes(), UUID.randomUUID().toString());
            }
            add(batch);
        }
    }};

    private byte[] getRandomBytes() {
        byte[] bytes = new byte[8];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    @Setup(Level.Iteration)
    public void setup() {
        final File path = new File(DB_PATH);
        if(!path.exists()){
            if(!path.mkdirs()){
                throw new RuntimeException(String.format("Unable to create the directory at the provided path: %s", path.getName()));
            }
        }
        mapDbPrepperState = new MapDbPrepperState<>(new File(DB_PATH), DB_NAME, DEFAULT_CONCURRENCY);

    }

    @TearDown(Level.Iteration)
    public void teardown() {
        mapDbPrepperState.delete();
    }

    @Benchmark
    @Fork(value = 1)
    @Warmup(iterations = 3)
    @Threads(value = 2)
    @Measurement(iterations = 5)
    public void benchmarkPutAll() {
        mapDbPrepperState.putAll(data.get(RANDOM.nextInt(NUM_BATCHES)));
    }


}
