/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;

import java.util.List;

public class MySqlStreamState {

    @JsonProperty("currentPosition")
    private BinlogCoordinate currentPosition;

    @JsonProperty("foreignKeyRelations")
    private List<ForeignKeyRelation> foreignKeyRelations;

    public BinlogCoordinate getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(BinlogCoordinate currentPosition) {
        this.currentPosition = currentPosition;
    }

    public List<ForeignKeyRelation> getForeignKeyRelations() {
        return foreignKeyRelations;
    }

    public void setForeignKeyRelations(List<ForeignKeyRelation> foreignKeyRelations) {
        this.foreignKeyRelations = foreignKeyRelations;
    }
}
