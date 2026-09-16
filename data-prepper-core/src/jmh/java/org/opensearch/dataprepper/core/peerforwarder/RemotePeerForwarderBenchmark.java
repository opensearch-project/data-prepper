/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Counter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class RemotePeerForwarderBenchmark {

    private static final int BUFFER_SIZE = 10240;
    private static final int BATCH_SIZE = 160;
    private static final int BATCH_DELAY = 100;
    private static final int FAILED_FORWARDING_REQUEST_LOCAL_WRITE_TIMEOUT = 100;
    private static final int FORWARDING_BATCH_SIZE = BATCH_SIZE;
    private static final int FORWARDING_BATCH_QUEUE_DEPTH = 25;
    private static final Duration FORWARDING_BATCH_TIMEOUT = Duration.ofMillis(800);
    private static final int PIPELINE_WORKER_THREADS = 8;
    private static final int HASH_RING_VIRTUAL_NODES = 128;

    private RemotePeerForwarder peerForwarder;
    private Collection<Record<Event>> testRecords;
    private ScheduledExecutorService networkLatencySimulator;

    @Param({"1", "2", "4"})
    private int nodeCount;

    @Param({"100", "1000", "5000", "50000"})
    private int recordCount;

    @Param({"0", "2", "5"})
    private int networkLatencyMs;

    @Setup(Level.Trial)
    public void setup() {
        // Create a thread pool for simulating network latency
        networkLatencySimulator = Executors.newScheduledThreadPool(PIPELINE_WORKER_THREADS * 2);

        PeerForwarderClient mockClient = mock(PeerForwarderClient.class);
        
        when(mockClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString()))
            .thenAnswer(invocation -> {
                CompletableFuture<AggregatedHttpResponse> future = new CompletableFuture<>();                
                if (networkLatencyMs == 0) {
                    future.complete(AggregatedHttpResponse.of(HttpStatus.OK));
                } else {
                    // Simulate network latency
                    networkLatencySimulator.schedule(
                        () -> future.complete(AggregatedHttpResponse.of(HttpStatus.OK)),
                        networkLatencyMs,
                        TimeUnit.MILLISECONDS
                    );
                }
                
                return future;
            });

        PeerListProvider peerListProvider = createPeerListProvider(nodeCount);
        HashRing hashRing = new HashRing(peerListProvider, HASH_RING_VIRTUAL_NODES);

        PeerForwarderReceiveBuffer<Record<Event>> buffer =
            new PeerForwarderReceiveBuffer<>(BUFFER_SIZE, BATCH_SIZE, "test", "test");

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);
        Counter mockCounter = mock(Counter.class);
        when(mockPluginMetrics.counter(anyString())).thenReturn(mockCounter);

        peerForwarder = new RemotePeerForwarder(
            mockClient, hashRing, buffer, "test", "test",
            Set.of("key1", "key2"), mockPluginMetrics,
            BATCH_DELAY, FAILED_FORWARDING_REQUEST_LOCAL_WRITE_TIMEOUT,
            FORWARDING_BATCH_SIZE, FORWARDING_BATCH_QUEUE_DEPTH,
            FORWARDING_BATCH_TIMEOUT, PIPELINE_WORKER_THREADS
        );

        testRecords = generateTestRecords(recordCount, nodeCount);
    }

    @TearDown(Level.Trial)
    public void teardown() {
        if (networkLatencySimulator != null) {
            networkLatencySimulator.shutdown();
            try {
                if (!networkLatencySimulator.awaitTermination(5, TimeUnit.SECONDS)) {
                    networkLatencySimulator.shutdownNow();
                }
            } catch (InterruptedException e) {
                networkLatencySimulator.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Benchmark
    public Collection<Record<Event>> benchmarkForwardRecords() {
        return peerForwarder.forwardRecords(testRecords);
    }

    private Collection<Record<Event>> generateTestRecords(int count, int nodes) {
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, String> data = new HashMap<>();
            data.put("key1", "value" + i);
            data.put("key2", "value" + (i % 10));
            records.add(new Record<>(JacksonLog.builder().withData(data).build()));
        }
        return records;
    }

    private PeerListProvider createPeerListProvider(int nodes) {
        List<String> ips = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            ips.add(i == 0 ? "127.0.0.1" : "10.0.0." + i);
        }

        return new PeerListProvider() {
            @Override
            public List<String> getPeerList() {
                return ips;
            }

            @Override
            public void addListener(Consumer<? super List<Endpoint>> listener) {
                // No-op for benchmark
            }

            @Override
            public void removeListener(Consumer<?> listener) {
                // No-op for benchmark
            }
        };
    }
}
