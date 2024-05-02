package org.opensearch.dataprepper.plugins.source.opensearch_api.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;
import org.opensearch.dataprepper.http.common.codec.Codec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiLineJsonCodec implements Codec<List<Map<String, Object>>>  {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};
    @Override
    public List<Map<String, Object>> parse(HttpData httpData) throws IOException {
        List<Map<String, Object>> jsonList = new ArrayList<>();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(httpData.toInputStream()));

        while(reader.ready()) {
            final String jsonLine = reader.readLine();
            if (jsonLine.isBlank() || jsonLine.isEmpty()) continue;
            jsonList.add(objectMapper.readValue(jsonLine, MAP_TYPE_REFERENCE));
        }

        return jsonList;
    }
}
