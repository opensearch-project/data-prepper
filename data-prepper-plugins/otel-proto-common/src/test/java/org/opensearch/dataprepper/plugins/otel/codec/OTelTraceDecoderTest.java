/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.junit.jupiter.api.Test;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.time.Instant;
import java.util.Objects;
import com.google.protobuf.util.JsonFormat;
import org.opensearch.dataprepper.model.trace.Span;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.binary.Hex;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;

public class OTelTraceDecoderTest {
    private static final String TEST_REQUEST_TRACES_FILE = "test-request-multiple-traces.json";
    
    public OTelTraceDecoder createObjectUnderTest(OTelOutputFormat outputFormat) {
        return new OTelTraceDecoder(outputFormat);
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelLogsDecoderTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    private ExportTraceServiceRequest buildExportTraceServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private void validateSpan(Span span) {
        String traceId = null;
        String spanId = null;
        try {
            traceId = new String(Hex.decodeHex(span.get("traceId", String.class)), StandardCharsets.UTF_8);
            spanId = new String(Hex.decodeHex(span.get("spanId", String.class)), StandardCharsets.UTF_8);
        } catch (Exception e) {
        }
        assertTrue(traceId.equals("TRACEID1") || traceId.equals("TRACEID2") || traceId.equals("TRACEID3"));
        if (traceId.equals("TRACEID1")) {
            assertTrue(spanId.equals("TRACEID1-SPAN1"));
        } else if (traceId.equals("TRACEID2")) {
            assertTrue(spanId.equals("TRACEID2-SPAN1") || spanId.equals("TRACEID2-SPAN2"));
        } else {
            assertTrue(spanId.equals("TRACEID3-SPAN1"));
        }
    }

    @Test
    public void testParse() throws Exception {
        final ExportTraceServiceRequest request = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TRACES_FILE);
        InputStream inputStream = new ByteArrayInputStream((byte[])request.toByteArray());
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH).parse(inputStream, Instant.now(), (record) -> {
            validateSpan((Span)record.getData());
        });
        
    }
}

