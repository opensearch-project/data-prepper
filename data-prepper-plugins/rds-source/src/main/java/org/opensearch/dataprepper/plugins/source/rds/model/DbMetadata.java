/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
@Builder
public class DbMetadata {

    static final String DB_IDENTIFIER_KEY = "dbIdentifier";
    static final String ENDPOINT_KEY = "endpoint";
    static final String PORT_KEY = "port";
    static final String READER_ENDPOINT_KEY = "readerEndpoint";
    static final String READER_PORT_KEY = "readerPort";

    private final String dbIdentifier;
    private final String endpoint;
    private final int port;
    private final String readerEndpoint;
    private final int readerPort;
    
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(DB_IDENTIFIER_KEY, dbIdentifier);
        map.put(ENDPOINT_KEY, endpoint);
        map.put(PORT_KEY, port);
        map.put(READER_ENDPOINT_KEY, readerEndpoint);
        map.put(READER_PORT_KEY, readerPort);

        return Collections.unmodifiableMap(map);
    }

    public static DbMetadata fromMap(Map<String, Object> map) {
        return new DbMetadata(
                (String) map.get(DB_IDENTIFIER_KEY),
                (String) map.get(ENDPOINT_KEY),
                (Integer) map.get(PORT_KEY),
                (String) map.get(READER_ENDPOINT_KEY),
                (Integer) map.get(READER_PORT_KEY)
        );
    }
}
