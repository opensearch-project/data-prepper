/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TraceTemplates {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();
    private static final Random RANDOM = new Random();

    public static List<String> exportTraceServiceRequestJsons(final int peerListSize, final int batchSize) {
        return getExportTraceServiceRequests(peerListSize, batchSize).stream().map(exportTraceServiceRequest -> {
            try {
                return PRINTER.print(exportTraceServiceRequest);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    public static List<ExportTraceServiceRequest> getExportTraceServiceRequests(final int peerListSize, final int batchSize) {
        final List<ExportTraceServiceRequest> results = new ArrayList<>();
        final List<ResourceSpans> resourceSpansList = getTraceGroupResourceSpans(peerListSize * batchSize);
        for (int i=0; i<peerListSize*batchSize; i+=batchSize) {
            results.add(getExportTraceServiceRequest(resourceSpansList.subList(i, i+batchSize)));
        }
        Collections.shuffle(results);
        return results;
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(final List<ResourceSpans> resourceSpansList) {
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(resourceSpansList)
                .build();
    }

    public static List<ResourceSpans> getTraceGroupResourceSpans(final int traceGroupSize) {
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        final byte[] traceId = getRandomBytes(16);
        final byte[] rootSpanId = getRandomBytes(8);
        for(int i = 0; i < traceGroupSize; i++) {
            final byte[] parentId = (i == 0? null : rootSpanId);
            final byte[] spanId = (i == 0? rootSpanId : getRandomBytes(8));
            final String serviceName = RandomStringUtils.randomAlphabetic(10);
            final String spanName = RandomStringUtils.randomAlphabetic(10);
            final Span.SpanKind spanKind = Span.SpanKind.SPAN_KIND_SERVER;
            final long endTime = System.currentTimeMillis() * 1000000;
            final long durationInNanos = 100000 + RANDOM.nextInt(500000);
            final ResourceSpans rs = getResourceSpans(
                    traceId,
                    spanId,
                    parentId,
                    serviceName,
                    spanName,
                    spanKind,
                    endTime,
                    durationInNanos,
                    1
            );
            spansList.add(rs);
        }
        return spansList;
    }

    public static ResourceSpans getResourceSpans(final byte[] traceId, final byte[] spanId, final byte[] parentId,
                                                 final String serviceName, final String spanName, final Span.SpanKind spanKind,
                                                 final long endTimeInNanos, final long durationInNanos, final Integer statusCode) {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        final long startTimeInNanos = endTimeInNanos - durationInNanos;
        final KeyValue.Builder kvBuilder = KeyValue.newBuilder()
                .setKey("key")
                .setValue(AnyValue.newBuilder().setStringValue("value"));
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
                                                .setStartTimeUnixNano(startTimeInNanos)
                                                .setEndTimeUnixNano(endTimeInNanos)
                                                .setStatus(Status.newBuilder().setCodeValue(statusCode))
                                                .addEvents(Span.Event.newBuilder()
                                                        .setName(spanName)
                                                        .setTimeUnixNano(endTimeInNanos)
                                                        .addAttributes(kvBuilder))
                                                .addLinks(Span.Link.newBuilder()
                                                        .setTraceId(ByteString.copyFrom(traceId))
                                                        .setSpanId(ByteString.copyFrom(spanId))
                                                        .setTraceState("test state")
                                                        .addAttributes(kvBuilder))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }
}
