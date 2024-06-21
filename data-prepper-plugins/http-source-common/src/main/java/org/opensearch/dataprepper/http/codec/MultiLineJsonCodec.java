package org.opensearch.dataprepper.http.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MultiLineJsonCodec implements Codec<List<Map<String, Object>>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String REGEX = "\\r?\\n";
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };
    private static final Pattern multiLineJsonSplitPattern = Pattern.compile(REGEX);

    private static boolean isInvalidLine(final String str) {
        return str == null || str.isEmpty() || str.isBlank();
    }

    @Override
    public List<Map<String, Object>> parse(HttpData httpData) throws IOException {
        List<Map<String, Object>> jsonListData = new ArrayList<>();

        String requestBody = new String(httpData.toInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String[] jsonLines = multiLineJsonSplitPattern.split(requestBody);

        for (String jsonLine : jsonLines) {
            if (isInvalidLine(jsonLine)) {
                throw new IOException("Error processing request payload.");
            }
            jsonListData.add(objectMapper.readValue(jsonLine, MAP_TYPE_REFERENCE));
        }
        return jsonListData;
    }
}
