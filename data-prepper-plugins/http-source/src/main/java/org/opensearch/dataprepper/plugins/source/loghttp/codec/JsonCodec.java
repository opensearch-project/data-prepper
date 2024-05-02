/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JsonCodec parses the json array format HTTP data into List&lt;{@link String}&gt;.
 * TODO: replace output List&lt;String&gt; with List&lt;InternalModel&gt; type
 * <p>
 */
public class JsonCodec implements Codec<List<List<String>>> {
    // To account for "[" and "]" when the list is converted to String
    private static final String OVERHEAD_CHARACTERS = "[]";
    // To account for "," when the list is converted to String
    private static final int COMMA_OVERHEAD_LENGTH = 1;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP_TYPE_REFERENCE =
            new TypeReference<List<Map<String, Object>>>() {};

    @Override
    public List<List<String>> parse(HttpData httpData) throws IOException {
        List<String> jsonList = new ArrayList<>();
        final List<Map<String, Object>> logList = mapper.readValue(httpData.toInputStream(),
                LIST_OF_MAP_TYPE_REFERENCE);
        for (final Map<String, Object> log: logList) {
            final String recordString = mapper.writeValueAsString(log);
            jsonList.add(recordString);
        }

        return List.of(jsonList);
    }

    @Override
    public List<List<String>> parse(HttpData httpData, int maxSize) throws IOException {
        List<List<String>> jsonList = new ArrayList<>();
        final List<Map<String, Object>> logList = mapper.readValue(httpData.toInputStream(),
                LIST_OF_MAP_TYPE_REFERENCE);
        int size = OVERHEAD_CHARACTERS.length();
        List<String> innerJsonList = new ArrayList<>();
        for (final Map<String, Object> log: logList) {
            final String recordString = mapper.writeValueAsString(log);
            if (size + recordString.length() > maxSize) {
                jsonList.add(innerJsonList);
                innerJsonList = new ArrayList<>();
                size = OVERHEAD_CHARACTERS.length();
            }
            innerJsonList.add(recordString);
            size += recordString.length() + COMMA_OVERHEAD_LENGTH;
        }
        if (size > OVERHEAD_CHARACTERS.length()) {
            jsonList.add(innerJsonList);
        }

        return jsonList;
    }
}
