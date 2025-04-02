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
import java.util.Map;
import com.google.protobuf.util.JsonFormat;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;

public class OTelLogsDecoderTest {
    private static final String TEST_REQUEST_LOGS_FILE = "test-request-multiple-logs.json";
    
    public OTelLogsDecoder createObjectUnderTest(OTelOutputFormat outputFormat) {
        return new OTelLogsDecoder(outputFormat);
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

    private ExportLogsServiceRequest buildExportLogsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportLogsServiceRequest.Builder builder = ExportLogsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private void validateLog(OpenTelemetryLog logRecord) {
        assertThat(logRecord.getServiceName(), is("service"));
        assertThat(logRecord.getTime(), is("2020-05-24T14:00:00Z"));
        assertThat(logRecord.getObservedTime(), is("2020-05-24T14:00:02Z"));
        assertThat(logRecord.getBody(), is("Log value"));
        assertThat(logRecord.getDroppedAttributesCount(), is(3));
        assertThat(logRecord.getSchemaUrl(), is("schemaurl"));
        assertThat(logRecord.getSeverityNumber(), is(5));
        assertThat(logRecord.getSeverityText(), is("Severity value"));
        assertThat(logRecord.getTraceId(), is("ba1a1c23b4093b63"));
        assertThat(logRecord.getSpanId(), is("2cc83ac90ebc469c"));
        Map<String, Object> mergedAttributes = logRecord.getAttributes();
        assertThat(mergedAttributes.keySet().size(), is(2));
        assertThat(mergedAttributes.get("log.attributes.statement@params"), is("us-east-1"));
        assertThat(mergedAttributes.get("resource.attributes.service@name"), is("service"));
    }

    @Test
    public void testParse() throws Exception {
        final ExportLogsServiceRequest request = buildExportLogsServiceRequestFromJsonFile(TEST_REQUEST_LOGS_FILE);
        InputStream inputStream = new ByteArrayInputStream((byte[])request.toByteArray());
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH).parse(inputStream, Instant.now(), (record) -> {
            validateLog((OpenTelemetryLog)record.getData());
        });
        
    }
}

