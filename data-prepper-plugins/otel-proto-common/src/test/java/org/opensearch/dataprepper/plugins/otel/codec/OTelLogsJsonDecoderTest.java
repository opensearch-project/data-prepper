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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class OTelLogsJsonDecoderTest {
    private static final String TEST_REQUEST_LOGS_FILE = "test-request-multiple-logs.json";
    
    public OTelLogsJsonDecoder createObjectUnderTest(OTelOutputFormat outputFormat) {
        return new OTelLogsJsonDecoder(outputFormat);
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
        InputStream inputStream = OTelLogsJsonDecoderTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_LOGS_FILE);
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH).parse(inputStream, Instant.now(), (record) -> {
            validateLog((OpenTelemetryLog)record.getData());
        });
        
    }
}
