/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import java.io.InputStream;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class OTelLogsInputCodecTest {
    private static final String TEST_REQUEST_LOGS_FILE = "test-request-multiple-logs.json";
   
    @Mock
    private OTelLogsInputCodecConfig config;
    @Mock   
    private OTelLogsInputCodec otelLogsCodec;

    @BeforeEach
    void setup() {
        config = new OTelLogsInputCodecConfig();
        otelLogsCodec = createObjectUnderTest();
    }

    public OTelLogsInputCodec createObjectUnderTest() {
        return new OTelLogsInputCodec(config);
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
        InputStream inputStream = OTelLogsInputCodecTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_LOGS_FILE);
        otelLogsCodec.parse(inputStream, (record) -> {
            validateLog((OpenTelemetryLog)record.getData());
        });
        
    }    
}
