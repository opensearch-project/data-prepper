/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.codec;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingOutputStream;
import com.linecorp.armeria.common.HttpData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * JsonCodec parses the json array format HTTP data into List&lt;{@link String}&gt;.
 * TODO: replace output List&lt;String&gt; with List&lt;InternalModel&gt; type
 * <p>
 */
public class JsonCodec implements Codec<List<String>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP_TYPE_REFERENCE =
            new TypeReference<List<Map<String, Object>>>() {
            };
    private static final JsonFactory JSON_FACTORY = new JsonFactory();


    @Override
    public List<String> parse(final HttpData httpData) throws IOException {
        final List<String> jsonList = new ArrayList<>();
        final List<Map<String, Object>> logList = mapper.readValue(httpData.toInputStream(),
                LIST_OF_MAP_TYPE_REFERENCE);
        for (final Map<String, Object> log : logList) {
            final String recordString = mapper.writeValueAsString(log);
            jsonList.add(recordString);
        }

        return jsonList;
    }

    @Override
    public void validate(final HttpData content) throws IOException {
        mapper.readValue(content.toInputStream(),
                LIST_OF_MAP_TYPE_REFERENCE);
    }

    @Override
    public void serializeSplit(final HttpData content, final Consumer<String> serializedBodyConsumer, final int splitLength) throws IOException {
        final InputStream contentInputStream = content.toInputStream();
        if (splitLength == 0) {
            performSerialization(contentInputStream, serializedBodyConsumer, Integer.MAX_VALUE);
        } else {
            performSerialization(contentInputStream, serializedBodyConsumer, splitLength);
        }
    }


    private void performSerialization(final InputStream inputStream,
                                      final Consumer<String> serializedBodyConsumer,
                                      final int splitLength) throws IOException {

        try (final JsonParser jsonParser = JSON_FACTORY.createParser(inputStream)) {
            if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
                throw new RuntimeException("Input is not a valid JSON array.");
            }

            JsonArrayWriter jsonArrayWriter = new JsonArrayWriter(splitLength, serializedBodyConsumer);

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final JsonGenerator objectJsonGenerator = JSON_FACTORY
                        .createGenerator(outputStream, JsonEncoding.UTF8);
                objectJsonGenerator.copyCurrentStructure(jsonParser);
                objectJsonGenerator.close();


                if (jsonArrayWriter.willExceedByWriting(outputStream)) {
                    jsonArrayWriter.close();

                    jsonArrayWriter = new JsonArrayWriter(splitLength, serializedBodyConsumer);

                }
                jsonArrayWriter.write(outputStream);
            }

            jsonArrayWriter.close();
        }
    }


    private static class JsonArrayWriter {
        private static final JsonFactory JSON_FACTORY = new JsonFactory().setCodec(mapper);
        private static final int BUFFER_SIZE = 16 * 1024;
        private static final String NECESSARY_CHARACTERS_TO_WRITE = ",]";
        private final CountingOutputStream countingOutputStream;
        private final ByteArrayOutputStream outputStream;
        private final int splitLength;
        private final Consumer<String> serializedBodyConsumer;
        private final JsonGenerator generator;
        private boolean hasItem = false;

        JsonArrayWriter(final int splitLength, final Consumer<String> serializedBodyConsumer) throws IOException {
            outputStream = new ByteArrayOutputStream(Math.min(splitLength, BUFFER_SIZE));
            countingOutputStream = new CountingOutputStream(outputStream);
            this.splitLength = splitLength;
            this.serializedBodyConsumer = serializedBodyConsumer;
            generator = JSON_FACTORY.createGenerator(countingOutputStream, JsonEncoding.UTF8);
            generator.writeStartArray();
        }

        boolean willExceedByWriting(final ByteArrayOutputStream byteArrayOutputStream) {
            final int lengthToWrite = byteArrayOutputStream.size();
            final long lengthOfDataWritten = countingOutputStream.getCount();
            return lengthToWrite + lengthOfDataWritten + NECESSARY_CHARACTERS_TO_WRITE.length() > splitLength;
        }

        void write(final ByteArrayOutputStream individualJsonLine) throws IOException {
            final String jsonLineString = individualJsonLine.toString(Charset.defaultCharset());
            generator.writeRawValue(jsonLineString);
            generator.flush();
            hasItem = true;
        }

        void close() throws IOException {
            if (hasItem) {
                generator.writeEndArray();
                generator.flush();
                final String resultJson = outputStream.toString(Charset.defaultCharset());

                serializedBodyConsumer.accept(resultJson);
            }

            generator.close();
            outputStream.close();
        }
    }
}
