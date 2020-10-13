package com.amazon.situp.benchmarks.processor;

import com.google.protobuf.ByteString;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.processor.OTelHelper;
import com.amazon.situp.plugins.processor.ServiceMapStatefulProcessor;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
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

@State(Scope.Thread)
public class ServiceMapStatefulProcessorBenchmarks {

    private ServiceMapStatefulProcessor serviceMapStatefulProcessor;
    private List<List<Record<ExportTraceServiceRequest>>> batches;
    private static final String DB_PATH = "data/benchmark";
    private static final int BATCH_SIZE = 100;
    private static final int NUM_BATCHES = 1000;
    private static final Random RANDOM = new Random();
    private static final List<String> serviceNames = Arrays.asList("FRONTEND", "BACKEND", "PAYMENT", "CHECKOUT", "DATABASE");

    @Setup(Level.Trial)
    public void setupServiceMapStatefulProcessor() {
        serviceMapStatefulProcessor = new ServiceMapStatefulProcessor(1000, new File(DB_PATH), Clock.systemDefaultZone());
    }

    @Setup(Level.Trial)
    public void setupBatches() throws UnsupportedEncodingException {
        batches = new ArrayList<>();
        for(int i=0; i<NUM_BATCHES; i++) {
            List<Record<ExportTraceServiceRequest>> batch = new ArrayList<>();
            for(int j=0; j<BATCH_SIZE; j++) {
                batch.add(new Record<>(getExportTraceServiceRequest(
                        getResourceSpans(
                                serviceNames.get(RANDOM.nextInt(serviceNames.size())),
                                UUID.randomUUID().toString(),
                                getRandomBytes(8),
                                getRandomBytes(8),
                                getRandomBytes(16),
                                Span.SpanKind.CLIENT
                ))));
            }
            batches.add(batch);
        }
    }

    @Benchmark
    @Fork(value = 1)
    @Warmup(iterations = 3)
    @Threads(value = 8)
    @Measurement(iterations = 5)
    public void benchmarkExecute() {
        serviceMapStatefulProcessor.execute(batches.get(RANDOM.nextInt(NUM_BATCHES)));
    }

    private static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind) throws UnsupportedEncodingException {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("service.name")
                                        .setValue(AnyValue.newBuilder().setStringValue(serviceName).build()).build())
                                .build()
                )
                .addInstrumentationLibrarySpans(
                        0,
                        InstrumentationLibrarySpans.newBuilder()
                                .addSpans(
                                        Span.newBuilder()
                                                .setName(spanName)
                                                .setKind(spanKind)
                                                .setSpanId(ByteString.copyFrom(spanId))
                                                .setParentSpanId(parentSpanId)
                                                .setTraceId(ByteString.copyFrom(traceId))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(ResourceSpans...spans){
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(Arrays.asList(spans))
                .build();
    }

}
