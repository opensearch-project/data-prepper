package org.opensearch.dataprepper.http.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MultiLineJsonCodec implements Codec<List<Map<String, Object>>>  {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String REGEX = "\\r?\\n";
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};

    @Override
    public List<Map<String, Object>> parse(HttpData httpData) throws IOException {
        List<Map<String, Object>> jsonListData = new ArrayList<>();

        String requestBody = new String(httpData.toInputStream().readAllBytes(), StandardCharsets.UTF_8);
        List<String> jsonLines = Arrays.asList(requestBody.split(REGEX));

        for (String jsonLine: jsonLines) {
            if (isInvalidLine(jsonLine)) {
                throw new IOException("Error processing request payload.");
            }
            jsonListData.add(objectMapper.readValue(jsonLine, MAP_TYPE_REFERENCE));
        }
        return jsonListData;
    }

    private static boolean isInvalidLine(final String str) {
        return str == null || str.isEmpty() || str.isBlank();
    }
}
