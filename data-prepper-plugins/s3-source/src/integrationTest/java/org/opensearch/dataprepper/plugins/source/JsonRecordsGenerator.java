/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.jsoninputcodec.JsonInputCodec;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

class JsonRecordsGenerator implements RecordsGenerator {

    public static final String EVENT_VERSION_FIELD = "eventVersion";
    public static final String EVENT_VERSION_VALUE = "1.0";
    public static final int KNOWN_FIELD_COUNT_PER_EVENT = 7;
    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    public void write(final int numberOfRecords, final OutputStream outputStream) throws IOException {
        try (final JsonGenerator jsonGenerator = jsonFactory
                .createGenerator(outputStream, JsonEncoding.UTF8)) {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeArrayFieldStart("Records");

            for (int i = 0; i < numberOfRecords; i++) {
                writeSingleRecord(jsonGenerator);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    @Override
    public InputCodec getCodec() {
        return new JsonInputCodec();
    }

    @Override
    public String getFileExtension() {
        return "json";
    }

    @Override
    public void assertEventIsCorrect(final Event event) {

        final Map<String, Object> messageMap = event.toMap();
        assertThat(messageMap, notNullValue());
        assertThat(messageMap.size(), greaterThanOrEqualTo(KNOWN_FIELD_COUNT_PER_EVENT));
        assertThat(messageMap.get(EVENT_VERSION_FIELD), equalTo(EVENT_VERSION_VALUE));
    }

    private void writeSingleRecord(final JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField(EVENT_VERSION_FIELD, EVENT_VERSION_VALUE);
        jsonGenerator.writeStringField("eventTime", Instant.now().toString());
        jsonGenerator.writeStringField("eventSource", "ec2.amazonaws.com");
        jsonGenerator.writeStringField("eventName", "StartInstances");
        jsonGenerator.writeStringField("awsRegion", "us-east-1");
        jsonGenerator.writeStringField("sourceIPAddress", "ec2-api-tools 1.6.12.2");
        jsonGenerator.writeStringField("userAgent", "us-east-1");

        jsonGenerator.writeObjectFieldStart("userIdentity");
        jsonGenerator.writeStringField("type", "IAMUser");
        jsonGenerator.writeStringField("principalId", UUID.randomUUID().toString());
        final String userName = UUID.randomUUID().toString();
        jsonGenerator.writeStringField("arn", "arn:aws:iam::123456789012:user/" + userName);
        jsonGenerator.writeStringField("accountId", "123456789012");
        jsonGenerator.writeStringField("userName", userName);
        jsonGenerator.writeEndObject();

        jsonGenerator.writeEndObject();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
