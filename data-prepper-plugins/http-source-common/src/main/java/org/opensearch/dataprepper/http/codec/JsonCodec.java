/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.codec;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingOutputStream;
import com.linecorp.armeria.common.HttpData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

    public void serialize(final List<String> jsonList,
                          final Consumer<String> serializedBodyConsumer,
                          final int splitLength) throws IOException {
        if (splitLength < 0)
            throw new IllegalArgumentException("The splitLength must be greater than or equal to 0.");

        if (splitLength == 0) {
            performSerialization(jsonList, serializedBodyConsumer, Integer.MAX_VALUE);
        } else {
            performSerialization(jsonList, serializedBodyConsumer, splitLength);
        }
    }

    private void performSerialization(final List<String> jsonList,
                                      final Consumer<String> serializedBodyConsumer,
                                      final int splitLength) throws IOException {

        JsonArrayWriter jsonArrayWriter = new JsonArrayWriter(splitLength, serializedBodyConsumer);

        for (final String individualJsonLine : jsonList) {
            if (jsonArrayWriter.willExceedByWriting(individualJsonLine)) {
                jsonArrayWriter.close();

                jsonArrayWriter = new JsonArrayWriter(splitLength, serializedBodyConsumer);

            }
            jsonArrayWriter.write(individualJsonLine);
        }

        jsonArrayWriter.close();
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

        boolean willExceedByWriting(final String individualJsonLine) {
            final int lengthToWrite = individualJsonLine.getBytes(StandardCharsets.UTF_8).length;
            final long lengthOfDataWritten = countingOutputStream.getCount();
            return lengthToWrite + lengthOfDataWritten + NECESSARY_CHARACTERS_TO_WRITE.length() > splitLength;
        }

        void write(final String individualJsonLine) throws IOException {
            final JsonNode jsonNode = mapper.readTree(individualJsonLine);
            generator.writeTree(jsonNode);
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
