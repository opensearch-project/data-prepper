/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import jakarta.json.spi.JsonProvider;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParser;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonProvider;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of the opensearch-java client's {@link JsonpMapper}. It can avoid duplicate
 * serialization by use of the {@link SerializedJson} interface. For values that inherit from
 * {@link SerializedJson}, it will not re-serialize these. For other values, it uses Jackson
 * serialization via the {@link JacksonJsonpMapper}.
 */
public class PreSerializedJsonpMapper implements JsonpMapper {

    private final JsonpMapper innerMapper;
    private final PreSerializedJsonProvider jsonProvider;

    public PreSerializedJsonpMapper() {
        innerMapper = new JacksonJsonpMapper();
        jsonProvider = new PreSerializedJsonProvider();
    }

    @Override
    public JsonProvider jsonProvider() {
        return jsonProvider;
    }

    @Override
    public <T> T deserialize(JsonParser parser, Class<T> clazz) {
        return innerMapper.deserialize(parser, clazz);
    }

    @Override
    public <T> void serialize(T value, JsonGenerator generator) {
        if(value instanceof SerializedJson) {
            if (! (generator instanceof PreSerializedJsonGenerator))
                throw new IllegalArgumentException("Unsupported JsonGenerator");

            final OutputStream outputStream = ((PreSerializedJsonGenerator) generator).outputStream;

            try {
                outputStream.write(((SerializedJson) value).getSerializedJson());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else {
            innerMapper.serialize(value, generator);
        }
    }

    private static class PreSerializedJsonGenerator extends JacksonJsonpGenerator {
        private OutputStream outputStream;

        public PreSerializedJsonGenerator(com.fasterxml.jackson.core.JsonGenerator generator, OutputStream outputStream) {
            super(generator);
            this.outputStream = outputStream;
        }
    }


    private static class PreSerializedJsonProvider extends JacksonJsonProvider {
        @Override
        public JsonGenerator createGenerator(OutputStream out) {
            try {
                return new PreSerializedJsonGenerator(jacksonJsonFactory().createGenerator(out), out);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
