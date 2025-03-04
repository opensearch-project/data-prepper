/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ServiceMapTestUtils {
    private static final long TEST_DURATION_IN_NANOS = 1000;
    private static final int TEST_STATUS_CODE = 1;
    private static final Random RANDOM = new Random();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static Future<Set<ServiceMapRelationship>> startExecuteAsync(ExecutorService threadpool, ServiceMapStatefulProcessor processor,
                                                                        Collection<Record<Event>> records) {
        return threadpool.submit(() -> {
            return processor.execute(records)
                    .stream()
                    .map(record -> {
                        try {
                            return OBJECT_MAPPER.readValue(record.getData().toJsonString(), ServiceMapRelationship.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toSet());
        });
    }

    /**
     * Creates a {@link org.opensearch.dataprepper.model.trace.Span} object with the given parameters
     * @param serviceName Resource name for the ResourceSpans object
     * @param spanName Span name for the single span in the ResourceSpans object
     * @param spanId Span id for the single span in the ResourceSpans object
     * @param parentId Parent id for the single span in the ResourceSpans object
     * @param spanKind Span kind for the single span in the ResourceSpans object
     * @return {@link org.opensearch.dataprepper.model.trace.Span} object with a single span constructed according to the parameters
     */
    public static Span getSpan(final String serviceName, final String spanName, final String
            spanId, final String parentId, final String traceId, final io.opentelemetry.proto.trace.v1.Span.SpanKind spanKind) {
        final Instant endInstant = Instant.now();
        final String endTime = endInstant.toString();
        final JacksonSpan.Builder builder = JacksonSpan.builder(true)
                .withSpanId(spanId)
                .withTraceId(traceId)
                .withTraceState("")
                .withParentSpanId(parentId)
                .withName(spanName)
                .withServiceName(serviceName)
                .withKind(spanKind.name())
                .withStartTime(endInstant.minusNanos(TEST_DURATION_IN_NANOS).toString())
                .withEndTime(endTime)
                .withTraceGroup(parentId.isEmpty()? null : spanName)
                .withDurationInNanos(TEST_DURATION_IN_NANOS);
        if (parentId.isEmpty()) {
            builder.withTraceGroupFields(
                    DefaultTraceGroupFields.builder()
                            .withStatusCode(TEST_STATUS_CODE)
                            .withDurationInNanos(TEST_DURATION_IN_NANOS)
                            .withEndTime(endTime)
                            .build()
            );
        } else {
            builder.withTraceGroupFields(
                    DefaultTraceGroupFields.builder().build());
        }
        return builder.build();
    }

}
