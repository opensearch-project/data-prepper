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

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OTelTracesJsonDecoderTest {

    private static final String TEST_REQUEST_TRACES_FILE = "test-request-multiple-traces.json";

    public OTelTracesJsonDecoder createObjectUnderTest(OTelOutputFormat outputFormat) {
        return new OTelTracesJsonDecoder(outputFormat);
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
        InputStream inputStream = OTelTracesJsonDecoderTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_TRACES_FILE);
        List<Record<Event>> parsedRecords = new ArrayList<>();

        createObjectUnderTest(OTelOutputFormat.OPENSEARCH).parse(inputStream, Instant.now(), parsedRecords::add);

        assertThat(parsedRecords.size(), equalTo(4));
        for (Record<Event> record : parsedRecords) {
            validateSpan((Span) record.getData());
        }
    }

    @Test
    public void testParseWithInvalidJson_ThrowsException() {
        InputStream invalidStream = new java.io.ByteArrayInputStream("{ invalid json }}}".getBytes());

        assertThrows(Exception.class, () ->
                createObjectUnderTest(OTelOutputFormat.OPENSEARCH).parse(invalidStream, Instant.now(), record -> {}));
    }
}
