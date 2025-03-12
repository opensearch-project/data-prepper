/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class StreamProgressState {

    @JsonProperty("engineType")
    private String engineType;

    @JsonProperty("waitForExport")
    private boolean waitForExport = false;

    /**
     * Map of table name to primary keys
     */
    @JsonProperty("primaryKeyMap")
    private Map<String, List<String>> primaryKeyMap;

    @JsonProperty("mySqlStreamState")
    private MySqlStreamState mySqlStreamState;

    @JsonProperty("postgresStreamState")
    private PostgresStreamState postgresStreamState;

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public Map<String, List<String>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    public void setPrimaryKeyMap(Map<String, List<String>> primaryKeyMap) {
        this.primaryKeyMap = primaryKeyMap;
    }

    public boolean shouldWaitForExport() {
        return waitForExport;
    }

    public void setWaitForExport(boolean waitForExport) {
        this.waitForExport = waitForExport;
    }

    public MySqlStreamState getMySqlStreamState() {
        return mySqlStreamState;
    }

    public void setMySqlStreamState(MySqlStreamState mySqlStreamState) {
        this.mySqlStreamState = mySqlStreamState;
    }

    public PostgresStreamState getPostgresStreamState() {
        return postgresStreamState;
    }

    public void setPostgresStreamState(PostgresStreamState postgresStreamState) {
        this.postgresStreamState = postgresStreamState;
    }
}
