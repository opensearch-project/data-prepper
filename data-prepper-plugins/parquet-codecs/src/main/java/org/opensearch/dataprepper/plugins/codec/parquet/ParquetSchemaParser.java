/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ParquetSchemaParser {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Schema parseSchemaFromJsonFile(final String location) throws IOException {
        final Map<?, ?> map = mapper.readValue(Paths.get(location).toFile(), Map.class);
        return getSchema(map);
    }

    public static Schema getSchema(Map<?, ?> map) throws JsonProcessingException {
        final Map schemaMap = new HashMap();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            schemaMap.put(entry.getKey(), entry.getValue());
        }
        final String schemaJson = mapper.writeValueAsString(schemaMap);
        return new Schema.Parser().parse(schemaJson);
    }
}