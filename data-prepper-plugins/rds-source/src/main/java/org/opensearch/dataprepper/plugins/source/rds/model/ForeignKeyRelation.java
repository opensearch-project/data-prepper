/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ForeignKeyRelation {
    // TODO: add java docs
    @JsonProperty("database_name")
    private final String databaseName;

    @JsonProperty("parent_table_name")
    private final String parentTableName;

    @JsonProperty("referenced_key_name")
    private final String referencedKeyName;

    @JsonProperty("child_table_name")
    private final String childTableName;

    @JsonProperty("foreign_key_name")
    private final String foreignKeyName;

    @JsonProperty("foreign_key_default_value")
    @Builder.Default
    private Object foreignKeyDefaultValue = null;

    @JsonProperty("update_action")
    private final ForeignKeyAction updateAction;

    @JsonProperty("delete_action")
    private final ForeignKeyAction deleteAction;

    @JsonCreator
    public ForeignKeyRelation(@JsonProperty("database_name") String databaseName,
                              @JsonProperty("parent_table_name") String parentTableName,
                              @JsonProperty("referenced_key_name") String referencedKeyName,
                              @JsonProperty("child_table_name") String childTableName,
                              @JsonProperty("foreign_key_name") String foreignKeyName,
                              @JsonProperty("foreign_key_default_value") Object foreignKeyDefaultValue,
                              @JsonProperty("update_action") ForeignKeyAction updateAction,
                              @JsonProperty("delete_action") ForeignKeyAction deleteAction) {
        this.databaseName = databaseName;
        this.parentTableName = parentTableName;
        this.referencedKeyName = referencedKeyName;
        this.childTableName = childTableName;
        this.foreignKeyName = foreignKeyName;
        this.foreignKeyDefaultValue = foreignKeyDefaultValue;
        this.updateAction = updateAction;
        this.deleteAction = deleteAction;
    }

    /**
     * Checks either update action or delete action is one of the cascading actions (CASCADE, SET_DEFAULT, SET_NULL).
     *
     * @param foreignKeyRelation The foreign key relation.
     * @return True if the foreign key relation contains a cascade action, false otherwise.
     */
    public static boolean containsCascadingAction(ForeignKeyRelation foreignKeyRelation) {
        return ForeignKeyAction.isCascadingAction(foreignKeyRelation.getUpdateAction()) ||
                ForeignKeyAction.isCascadingAction(foreignKeyRelation.getDeleteAction());
    }
}