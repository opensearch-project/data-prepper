/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

import java.time.Instant;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OTelLogsProtoBufDecoderTest {
    private static final String TEST_REQUEST_JSON_LOGS_FILE = "test-request-multiple-logs.json";
    // This protobuf format file is generated using OTEL collector and s3 exporter. S3 object created is copied as file
    private static final String TEST_REQUEST_LOGS_FILE = "test-otel-log.protobuf";
    // This protobuf format file is generated using OTEL collector and file exporter and then sending multiple log events to the collector
    private static final String TEST_REQUEST_MULTI_LOGS_FILE = "test-otel-multi-log.protobuf";

    public OTelLogsProtoBufDecoder createObjectUnderTest(OTelOutputFormat outputFormat, boolean lengthPrefixedEncoding) {
        return new OTelLogsProtoBufDecoder(outputFormat, lengthPrefixedEncoding);
    }

    private void assertLog(OpenTelemetryLog logRecord, final int severityNumber, final String time, final String spanId) {
        assertThat(logRecord.getServiceName(), is("my.service"));
        assertThat(logRecord.getTime(), is(time));
        assertThat(logRecord.getObservedTime(), is(time));
        assertThat(logRecord.getBody(), is("Example log record"));
        assertThat(logRecord.getDroppedAttributesCount(), is(0));
        assertThat(logRecord.getSchemaUrl(), is(""));
        assertThat(logRecord.getSeverityNumber(), is(severityNumber));
        assertThat(logRecord.getFlags(), is(0));
        assertThat(logRecord.getSeverityText(), is("Information"));
        assertThat(logRecord.getSpanId(), is(spanId));
        assertThat(logRecord.getTraceId(), is("5b8efff798038103d269b633813fc60c"));
        Map<String, Object> mergedAttributes = logRecord.getAttributes();
        assertThat(mergedAttributes.keySet().size(), is(10));
    }

    @Test
    public void testParse() throws Exception {
        InputStream inputStream = OTelLogsProtoBufDecoderTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_LOGS_FILE);
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH, false).parse(inputStream, Instant.now(), (record) -> {
            assertLog((OpenTelemetryLog)record.getData(), 50, "2025-01-26T20:07:20Z", "eee19b7ec3c1b174");
        });
    }

    @Test
    public void testParseWithLengthPrefixedEncoding() throws Exception {
        InputStream inputStream = OTelLogsProtoBufDecoderTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_MULTI_LOGS_FILE);
        List<Record<Event>> parsedRecords = new ArrayList<>();
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH, true).parse(inputStream, Instant.now(), (record) -> {
            parsedRecords.add(record);
        });
        assertThat(parsedRecords.size(), equalTo(3));
        assertLog((OpenTelemetryLog)parsedRecords.get(0).getData(), 50, "2025-01-26T20:07:20Z", "eee19b7ec3c1b174");
        assertLog((OpenTelemetryLog)parsedRecords.get(1).getData(), 42, "2025-01-26T20:07:20Z", "eee19b7ec3c1b174");
        assertLog((OpenTelemetryLog)parsedRecords.get(2).getData(), 43, "2025-01-26T20:07:40Z", "fff19b7ec3c1b174");
    }

    private void assertLogFromRequest(OpenTelemetryLog logRecord) {
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

    private ExportLogsServiceRequest buildExportLogsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportLogsServiceRequest.Builder builder = ExportLogsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelLogsProtoBufDecoderTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    @Test
    public void testParseWithDynamicRequest() throws Exception {
        final ExportLogsServiceRequest exportLogsServiceRequest = buildExportLogsServiceRequestFromJsonFile(TEST_REQUEST_JSON_LOGS_FILE);
        InputStream inputStream = new ByteArrayInputStream(exportLogsServiceRequest.toByteArray());
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH, false).parse(inputStream, Instant.now(), (record) -> {
            assertLogFromRequest((OpenTelemetryLog)record.getData());
        });
    }

    @Test
    public void testParseWithLargeDynamicRequest_ThrowsException() throws Exception {

        // Create a request larger than 8MB
        List<LogRecord> records = new ArrayList<>();
        for (int i = 0; i < 4 * 1024 * 1024; i++) {
            records.add(LogRecord.newBuilder().build());
        }
        ExportLogsServiceRequest exportLogsServiceRequest = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder()
                    .addScopeLogs(ScopeLogs.newBuilder()
                        .addAllLogRecords(records)
                    .build())).build();

        InputStream inputStream = new ByteArrayInputStream(exportLogsServiceRequest.toByteArray());
        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest(OTelOutputFormat.OPENSEARCH, false).parse(inputStream, Instant.now(), (record) -> {
            assertLogFromRequest((OpenTelemetryLog)record.getData());
        }));
    }

}
