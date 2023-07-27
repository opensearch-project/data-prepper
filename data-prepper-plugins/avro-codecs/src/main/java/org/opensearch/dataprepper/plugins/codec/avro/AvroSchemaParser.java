/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchemaParser {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(AvroOutputCodec.class);

    public static Schema parseSchemaFromJsonFile(final String location) throws IOException {
        final Map<?, ?> jsonMap;
        try {
            jsonMap = mapper.readValue(Paths.get(location).toFile(), Map.class);
        } catch (FileNotFoundException e) {
            LOG.error("Schema file not found, Error: {}", e.getMessage());
            throw new IOException("Can't proceed without schema.");
        }
        final Map<Object,Object> schemaMap = new HashMap<Object,Object>();
        for (Map.Entry<?, ?> entry : jsonMap.entrySet()) {
            schemaMap.put(entry.getKey(), entry.getValue());
        }
        try{
            return new Schema.Parser().parse(mapper.writeValueAsString(schemaMap));
        }catch(Exception e) {
            LOG.error("Unable to parse schema from the provided schema file, Error: {}", e.getMessage());
            throw new IOException("Can't proceed without schema.");
        }
    }
}
