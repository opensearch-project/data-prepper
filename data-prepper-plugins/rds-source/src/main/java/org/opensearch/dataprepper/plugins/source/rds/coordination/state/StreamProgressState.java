/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;

import java.util.List;
import java.util.Map;

public class StreamProgressState {

    // TODO: separate MySQL and Postgres properties into different progress state classes
    // Common
    @JsonProperty("engineType")
    private String engineType;

    @JsonProperty("waitForExport")
    private boolean waitForExport = false;

    /**
     * Map of table name to primary keys
     */
    @JsonProperty("primaryKeyMap")
    private Map<String, List<String>> primaryKeyMap;

    // For MySQL
    @JsonProperty("currentPosition")
    private BinlogCoordinate currentPosition;

    @JsonProperty("foreignKeyRelations")
    private List<ForeignKeyRelation> foreignKeyRelations;

    // For Postgres
    @JsonProperty("currentLsn")
    private String currentLsn;

    @JsonProperty("replicationSlotName")
    private String replicationSlotName;

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public BinlogCoordinate getCurrentPosition() {
        return currentPosition;
    }

    public String getCurrentLsn() {
        return currentLsn;
    }

    public Map<String, List<String>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    public void setPrimaryKeyMap(Map<String, List<String>> primaryKeyMap) {
        this.primaryKeyMap = primaryKeyMap;
    }

    public String getReplicationSlotName() {
        return replicationSlotName;
    }

    public void setCurrentPosition(BinlogCoordinate currentPosition) {
        this.currentPosition = currentPosition;
    }

    public void setReplicationSlotName(String replicationSlotName) {
        this.replicationSlotName = replicationSlotName;
    }

    public boolean shouldWaitForExport() {
        return waitForExport;
    }

    public void setWaitForExport(boolean waitForExport) {
        this.waitForExport = waitForExport;
    }

    public List<ForeignKeyRelation> getForeignKeyRelations() {
        return foreignKeyRelations;
    }

    public void setForeignKeyRelations(List<ForeignKeyRelation> foreignKeyRelations) {
        this.foreignKeyRelations = foreignKeyRelations;
    }
}
