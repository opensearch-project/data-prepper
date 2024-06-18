/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.common.lambda.codec;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

public class LambdaJsonCodec implements OutputCodec {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String JSON = "json";
    private static final JsonFactory factory = new JsonFactory();
    private JsonGenerator generator;
    private OutputCodecContext codecContext;
    private final String keyName;

    public LambdaJsonCodec(final String keyName) {
        this.keyName = keyName;
    }

    @Override
    public String getExtension() {
        return JSON;
    }

    @Override
    public void start(final OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        this.codecContext = codecContext;
        generator = factory.createGenerator(outputStream, JsonEncoding.UTF8);
        if(Objects.nonNull(keyName)){
            generator.writeStartObject();
            generator.writeFieldName(keyName);
            generator.writeStartArray();
        }
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        if(!Objects.isNull(keyName)) {
            generator.writeEndArray();
            generator.writeEndObject();
        }

        generator.close();
        outputStream.flush();
        outputStream.close();
    }

    @Override
    public synchronized void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        if(Objects.isNull(keyName)) {
            Map<String, Object> eventMap = event.toMap();
            objectMapper.writeValue(outputStream, eventMap);

        }else{
            Map<String, Object> dataMap = event.toMap(); //(event);
            objectMapper.writeValue(generator, dataMap);
        }
            generator.flush();
    }
}




