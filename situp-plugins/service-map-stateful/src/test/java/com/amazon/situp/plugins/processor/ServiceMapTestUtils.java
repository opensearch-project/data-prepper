package com.amazon.situp.plugins.processor;

import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ServiceMapTestUtils {

    private static final Random RANDOM = new Random();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static byte[] getSpanId(ResourceSpans resourceSpans) {
        return resourceSpans.getInstrumentationLibrarySpans(0).getSpans(0).getSpanId().toByteArray();
    }

    public static Future<Set<ServiceMapRelationship>> startExecuteAsync(ExecutorService threadpool, ServiceMapStatefulProcessor processor,
                                                                 Collection<Record<ExportTraceServiceRequest>> records) {
        return threadpool.submit(() -> {
            return processor.execute(records)
                    .stream()
                    .map(record -> {
                        try {
                            return OBJECT_MAPPER.readValue(record.getData(), ServiceMapRelationship.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toSet());
        });
    }

    /**
     * Creates a ResourceSpans object with the given parameters, with a single span
     * @param serviceName Resource name for the ResourceSpans object
     * @param spanName Span name for the single span in the ResourceSpans object
     * @param spanId Span id for the single span in the ResourceSpans object
     * @param parentId Parent id for the single span in the ResourceSpans object
     * @param spanKind Span kind for the single span in the ResourceSpans object
     * @return ResourceSpans object with a single span constructed according to the parameters
     * @throws UnsupportedEncodingException
     */
    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind) throws UnsupportedEncodingException {
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("resource.name")
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
                                                .setParentSpanId(ByteString.copyFrom(parentId))
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
