/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;

import java.util.List;

public class StreamProgressState {

    @JsonProperty("currentPosition")
    private BinlogCoordinate currentPosition;

    @JsonProperty("currentLsn")
    private String currentLsn;

    @JsonProperty("replicationSlotName")
    private String replicationSlotName;

    @JsonProperty("waitForExport")
    private boolean waitForExport = false;

    @JsonProperty("foreignKeyRelations")
    private List<ForeignKeyRelation> foreignKeyRelations;

    public BinlogCoordinate getCurrentPosition() {
        return currentPosition;
    }

    public String getCurrentLsn() {
        return currentLsn;
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
