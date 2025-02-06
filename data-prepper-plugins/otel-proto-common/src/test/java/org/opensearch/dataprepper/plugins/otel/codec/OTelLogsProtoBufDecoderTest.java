/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.event.Event;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class OTelLogsProtoBufDecoderTest {
    private static final String TEST_REQUEST_LOGS_FILE = "test-otel-log.protobuf";
    private static final String TEST_REQUEST_MULTI_LOGS_FILE = "test-otel-multi-log.protobuf";
    private int count;
    
    public OTelLogsProtoBufDecoder createObjectUnderTest(boolean lengthPrefixedEncoding) {
        return new OTelLogsProtoBufDecoder(lengthPrefixedEncoding);
    }

    private void validateLog(OpenTelemetryLog logRecord, final int severityNumber, final String time, final String spanId) {
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
        assertThat(mergedAttributes.keySet().size(), is(9)); 
    }

    @Test
    public void testParse() throws Exception {
        InputStream inputStream = OTelLogsProtoBufDecoderTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_LOGS_FILE);
        createObjectUnderTest(false).parse(inputStream, Instant.now(), (record) -> {
            validateLog((OpenTelemetryLog)record.getData(), 50, "2025-01-26T20:07:20Z", "eee19b7ec3c1b174");
        });
        
    }

    @Test
    public void testParseWithLengthPrefixedEncoding() throws Exception {
        InputStream inputStream = OTelLogsProtoBufDecoderTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_MULTI_LOGS_FILE);
        count = 0;
        createObjectUnderTest(true).parse(inputStream, Instant.now(), (record) -> {
            Event event = (Event)record.getData();
            int severityNumber = (count == 0) ? 50 : (count == 1) ? 42 : 43;
            String time = (count < 2) ? "2025-01-26T20:07:20Z" : "2025-01-26T20:07:40Z";
            String spanId = (count < 2) ? "eee19b7ec3c1b174" : "fff19b7ec3c1b174";
            validateLog((OpenTelemetryLog)record.getData(), severityNumber, time, spanId);
            count++;
        });
        
    }
}
