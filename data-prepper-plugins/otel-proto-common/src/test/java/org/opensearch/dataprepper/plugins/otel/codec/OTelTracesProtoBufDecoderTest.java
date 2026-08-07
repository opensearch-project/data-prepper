/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OTelTracesProtoBufDecoderTest {

    private static final String TEST_REQUEST_JSON_TRACES_FILE = "test-request-multiple-traces.json";

    public OTelTracesProtoBufDecoder createObjectUnderTest(OTelOutputFormat outputFormat, boolean lengthPrefixedEncoding) {
        return new OTelTracesProtoBufDecoder(outputFormat, lengthPrefixedEncoding);
    }

    private void validateSpan(Span span) {
        assertThat(span.getServiceName(), is("analytics-service1"));
        assertThat(span.getTraceId(), notNullValue());
        assertThat(span.getSpanId(), notNullValue());
        assertThat(span.getName(), notNullValue());
        assertThat(span.getStartTime(), notNullValue());
        assertThat(span.getEndTime(), notNullValue());
        assertThat(span.getDurationInNanos(), notNullValue());
    }

    @Test
    public void testParse() throws Exception {
        final ExportTraceServiceRequest request = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_JSON_TRACES_FILE);
        InputStream inputStream = new ByteArrayInputStream(request.toByteArray());
        List<Record<Event>> parsedRecords = new ArrayList<>();

        createObjectUnderTest(OTelOutputFormat.OPENSEARCH, false).parse(inputStream, Instant.now(), parsedRecords::add);

        assertThat(parsedRecords.size(), equalTo(4));
        for (Record<Event> record : parsedRecords) {
            validateSpan((Span) record.getData());
        }
    }

    @Test
    public void testParseWithLengthPrefixedEncoding() throws Exception {
        final ExportTraceServiceRequest request = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_JSON_TRACES_FILE);
        byte[] requestBytes = request.toByteArray();

        // Generate length-prefixed protobuf on-the-fly (mimics OTel file exporter proto format)
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (int i = 0; i < 3; i++) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            lengthBuffer.putInt(requestBytes.length);
            outputStream.write(lengthBuffer.array());
            outputStream.write(requestBytes);
        }

        List<Record<Event>> parsedRecords = new ArrayList<>();
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH, true)
                .parse(new ByteArrayInputStream(outputStream.toByteArray()), Instant.now(), parsedRecords::add);

        // 3 requests × 4 spans each = 12 spans
        assertThat(parsedRecords.size(), equalTo(12));
        validateSpan((Span) parsedRecords.get(0).getData());
    }

    @Test
    public void testParseWithLargeDynamicRequest_ThrowsException() throws Exception {
        List<io.opentelemetry.proto.trace.v1.Span> spans = new ArrayList<>();
        for (int i = 0; i < 4 * 1024 * 1024; i++) {
            spans.add(io.opentelemetry.proto.trace.v1.Span.newBuilder().build());
        }
        ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addAllSpans(spans)
                                .build()))
                .build();

        InputStream inputStream = new ByteArrayInputStream(request.toByteArray());

        assertThrows(IllegalArgumentException.class, () ->
                createObjectUnderTest(OTelOutputFormat.OPENSEARCH, false)
                        .parse(inputStream, Instant.now(), record -> {}));
    }

    private ExportTraceServiceRequest buildExportTraceServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelTracesProtoBufDecoderTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }
}
