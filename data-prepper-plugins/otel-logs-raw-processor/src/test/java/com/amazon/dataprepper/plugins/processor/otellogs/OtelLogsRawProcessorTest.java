/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otellogs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.InstrumentationLibraryLogs;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.commons.codec.binary.Hex;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class OtelLogsRawProcessorTest {

    private static final Random RANDOM = new Random();

    private static final Long START_TIME = TimeUnit.MILLISECONDS.toNanos(ZonedDateTime.of(
            LocalDateTime.of(2020, 5, 24, 14, 0, 0),
            ZoneOffset.UTC).toInstant().toEpochMilli());

    private static final Long OBSERVED_TIME = TimeUnit.MILLISECONDS.toNanos(ZonedDateTime.of(
            LocalDateTime.of(2020, 5, 24, 14, 0, 2),
            ZoneOffset.UTC).toInstant().toEpochMilli());

    private static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static final byte[] SPAN_ID = getRandomBytes(8);

    private static final byte[] TRACE_ID = getRandomBytes(8);

    private static final LogRecord LOG_RECORD = LogRecord.newBuilder()
            .setFlags(1)
            .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_DEBUG)
            .setSpanId(ByteString.copyFrom(SPAN_ID))
            .setTraceId(ByteString.copyFrom(TRACE_ID))
            .addAttributes(
                    KeyValue.newBuilder()
                            .setKey("statement.params")
                            .setValue(AnyValue.newBuilder()
                                    .setStringValue("us-east-1").build()).build())
            .setDroppedAttributesCount(3)
            .setTimeUnixNano(START_TIME)
            .setObservedTimeUnixNano(OBSERVED_TIME)
            .setBody(AnyValue.newBuilder()
                    .setStringValue("Log value")
                    .build())
            .build();

    private static final ExportLogsServiceRequest LOG = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder()
                    .addInstrumentationLibraryLogs(InstrumentationLibraryLogs.newBuilder()
                            .addLogRecords(LOG_RECORD)
                            .build())
                    .addScopeLogs(ScopeLogs.newBuilder()
                            .addLogRecords(LOG_RECORD)
                            .build())
                    .setResource(Resource.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                    .setKey("service.name")
                                    .setValue(AnyValue.newBuilder().setStringValue("service").build())
                            ).build())
                    .setSchemaUrl("schemaurl")
                    .build())
            .build();

    private OTelLogsRawProcessor rawProcessor;


    @Before
    public void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        rawProcessor = new OTelLogsRawProcessor(testsettings);
    }

    @Test
    public void test() throws JsonProcessingException {
        List<Record<? extends OpenTelemetryLog>> processedRecords = (List<Record<? extends OpenTelemetryLog>>)
                rawProcessor.doExecute(Collections.singletonList(new Record<>(LOG)));

        assertThat(processedRecords).hasSize(2);
        ObjectMapper objectMapper = new ObjectMapper();
        assertLog(objectMapper.readValue(processedRecords.get(0).getData().toJsonString(), Map.class));
        assertLog(objectMapper.readValue(processedRecords.get(1).getData().toJsonString(), Map.class));
    }

    private void assertLog(Map<Object, Object> mappedLog) {
        assertThat(mappedLog).contains(entry("time", "2020-05-24T14:00:00Z"));
        assertThat(mappedLog).contains(entry("observedTime", "2020-05-24T14:00:02Z"));
        assertThat(mappedLog).contains(entry("serviceName", "service"));
        assertThat(mappedLog).contains(entry("flags", 1));
        assertThat(mappedLog).contains(entry("traceId", Hex.encodeHexString(TRACE_ID)));
        assertThat(mappedLog).contains(entry("spanId", Hex.encodeHexString(SPAN_ID)));
        assertThat(mappedLog).contains(entry("droppedAttributesCount", 3));
        assertThat(mappedLog).contains(entry("body", "Log value"));
        assertThat(mappedLog).contains(entry("schemaUrl", "schemaurl"));
        assertThat(mappedLog).contains(entry("log.attributes.statement@params", "us-east-1"));
        assertThat(mappedLog).contains(entry("resource.attributes.service@name", "service"));
    }
}
