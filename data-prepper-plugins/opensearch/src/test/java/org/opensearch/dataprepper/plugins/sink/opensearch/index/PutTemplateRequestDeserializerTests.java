package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import jakarta.json.stream.JsonParser;
import org.junit.Test;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.Assert.assertNotNull;

public class PutTemplateRequestDeserializerTests {
    private static final String TEST_PUT_TEMPLATE_REQUEST_JSON_FILE = "test-put-template-request.json";

    private String readDocFromFile(final String filename) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(filename))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    @Test
    public void testGetJsonpDeserializer() throws IOException {
        final JsonpMapper jsonpMapper = new PreSerializedJsonpMapper();
        final String putTemplateRequestJson = readDocFromFile(TEST_PUT_TEMPLATE_REQUEST_JSON_FILE);
        final JsonpDeserializer<PutTemplateRequest> objectUnderTest = PutTemplateRequestDeserializer
                .getJsonpDeserializer();
        final ByteArrayInputStream byteIn = new ByteArrayInputStream(
                putTemplateRequestJson.getBytes(StandardCharsets.UTF_8));
        final JsonParser parser = jsonpMapper.jsonProvider().createParser(byteIn);
        final PutTemplateRequest putTemplateRequest = objectUnderTest.deserialize(parser, jsonpMapper);
        assertNotNull(putTemplateRequest.name());
        assertNotNull(putTemplateRequest.aliases());
        assertNotNull(putTemplateRequest.indexPatterns());
        assertNotNull(putTemplateRequest.settings());
        assertNotNull(putTemplateRequest.mappings());
        assertNotNull(putTemplateRequest.version());
    }
}