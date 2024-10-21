/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A data model for a parent table in a foreign key relationship
 */
@Getter
@Builder
public class ParentTable {
    private final String databaseName;
    private final String tableName;
    /**
     * Column name to a list of ForeignKeyRelation in which the column is referenced
     */
    private final Map<String, List<ForeignKeyRelation>> referencedColumnMap;

    @Getter(AccessLevel.NONE)
    @Builder.Default
    private Map<String, List<ForeignKeyRelation>> columnsWithCascadingUpdate = null;

    @Getter(AccessLevel.NONE)
    @Builder.Default
    private Map<String, List<ForeignKeyRelation>> columnsWithCascadingDelete = null;

    /**
     * Returns a map of column name to a list of ForeignKeyRelation in which the column is referenced and the update action is cascading.
     * @return a map of column name to a list of ForeignKeyRelation
     */
    public Map<String, List<ForeignKeyRelation>> getColumnsWithCascadingUpdate() {
        if (columnsWithCascadingUpdate != null) {
            return columnsWithCascadingUpdate;
        }

        final Map<String, List<ForeignKeyRelation>> columnsWithCascadingUpdate = new HashMap<>();
        for (String column : referencedColumnMap.keySet()) {
            for (ForeignKeyRelation foreignKeyRelation : referencedColumnMap.get(column)) {
                if (ForeignKeyAction.isCascadingAction(foreignKeyRelation.getUpdateAction())) {
                    if (!columnsWithCascadingUpdate.containsKey(column)) {
                        columnsWithCascadingUpdate.put(column, new ArrayList<>());
                    }
                    columnsWithCascadingUpdate.get(column).add(foreignKeyRelation);
                }
            }
        }
        return columnsWithCascadingUpdate;
    }

    /**
     * Returns a map of column name to a list of ForeignKeyRelation in which the column is referenced and the delete action is cascading.
     * @return a map of column name to a list of ForeignKeyRelation
     */
    public Map<String, List<ForeignKeyRelation>> getColumnsWithCascadingDelete() {
        if (columnsWithCascadingDelete != null) {
            return columnsWithCascadingDelete;
        }

        final Map<String, List<ForeignKeyRelation>> columnsWithCascadingDelete = new HashMap<>();
        for (String column : referencedColumnMap.keySet()) {
            for (ForeignKeyRelation foreignKeyRelation : referencedColumnMap.get(column)) {
                if (ForeignKeyAction.isCascadingAction(foreignKeyRelation.getDeleteAction())) {
                    if (!columnsWithCascadingDelete.containsKey(column)) {
                        columnsWithCascadingDelete.put(column, new ArrayList<>());
                    }
                    columnsWithCascadingDelete.get(column).add(foreignKeyRelation);
                }
            }
        }
        return columnsWithCascadingDelete;
    }
}
