package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class PayloadValidator {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static InputStream validateAndGetInputStream(SdkBytes payload) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(payload.asByteArray());

        if (!jsonNode.isArray()) {
            throw new IllegalArgumentException("Payload must be a JSON array");
        }

        return new ByteArrayInputStream(payload.asByteArray());
    }
}
