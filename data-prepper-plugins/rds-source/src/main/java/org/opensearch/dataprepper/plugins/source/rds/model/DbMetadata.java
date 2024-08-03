/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Map;

public class DbMetadata {

    private static final String DB_IDENTIFIER_KEY = "dbIdentifier";
    private static final String HOST_NAME_KEY = "hostName";
    private static final String PORT_KEY = "port";
    private final String dbIdentifier;
    private final String hostName;
    private final int port;

    public DbMetadata(final String dbIdentifier, final String hostName, final int port) {
        this.dbIdentifier = dbIdentifier;
        this.hostName = hostName;
        this.port = port;
    }
    
    public String getDbIdentifier() {
        return dbIdentifier;
    }
    
    public String getHostName() {
        return hostName;
    }
    
    public int getPort() {
        return port;
    }
    
    public Map<String, Object> toMap() {
        return Map.of(
                DB_IDENTIFIER_KEY, dbIdentifier,
                HOST_NAME_KEY, hostName,
                PORT_KEY, port
        );
    }
}
