/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.plugins.source.rds.configuration.JoinRelation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinMetadataEnricher {

    static final String JOIN_TABLE_METADATA = "_table";
    static final String JOIN_FIELDS_METADATA = "_fields";
    static final String JOIN_IS_DELETE_METADATA = "_is_delete";
    static final String JOIN_IS_PARENT_METADATA = "_is_parent";
    static final String JOIN_PRIMARY_KEY_METADATA = MetadataKeyAttributes.JOIN_PRIMARY_KEY_METADATA;
    static final String JOIN_CHILD_TABLE_NAME_METADATA = "_child_table_name";
    static final String JOIN_CHILD_PK_NAME_METADATA = "_child_pk_name";
    static final String JOIN_CHILD_PK_VALUE_METADATA = "_child_pk_value";
    static final String JOIN_TYPE_METADATA = "_join_type";
    static final String JOIN_MAX_CHILD_RECORDS_METADATA = "_max_child_records";

    private final Map<String, JoinRelation> parentTableRelations;
    private final Map<String, JoinRelation> childTableRelations;

    public JoinMetadataEnricher(final List<JoinRelation> relations) {
        parentTableRelations = new HashMap<>();
        childTableRelations = new HashMap<>();
        if (relations != null) {
            for (final JoinRelation relation : relations) {
                parentTableRelations.put(relation.getParentTable(), relation);
                childTableRelations.put(relation.getChildTable(), relation);
            }
        }
    }

    public boolean isJoinTable(final String tableName) {
        return parentTableRelations.containsKey(tableName) || childTableRelations.containsKey(tableName);
    }

    /**
     * Returns the child key columns for a child table, or null if not a child table.
     * Used to detect FK changes in UPDATE events.
     */
    public List<String> getChildKeyColumns(final String tableName) {
        final JoinRelation relation = childTableRelations.get(tableName);
        return relation != null ? relation.getChildKey() : null;
    }

    public void enrich(final Event event, final String tableName, final List<String> columnNames, final boolean isDelete) {
        final JoinRelation relation = getRelation(tableName);
        if (relation == null) {
            return;
        }

        final boolean isParent = parentTableRelations.containsKey(tableName);
        final EventMetadata metadata = event.getMetadata();

        // Filter out join key columns from fields
        final Set<String> excludeKeys = getExcludeKeys(relation, isParent);
        final String fields = columnNames.stream()
                .filter(col -> !excludeKeys.contains(col))
                .collect(Collectors.joining(","));

        metadata.setAttribute(JOIN_TABLE_METADATA, tableName);
        metadata.setAttribute(JOIN_FIELDS_METADATA, fields);
        metadata.setAttribute(JOIN_IS_DELETE_METADATA, String.valueOf(isDelete));
        metadata.setAttribute(JOIN_IS_PARENT_METADATA, String.valueOf(isParent));

        // Document ID: composite key values joined with |
        final List<String> parentKeyFields = isParent ? relation.getParentKey() : relation.getChildKey();
        final String primaryKeyValue = parentKeyFields.stream()
                .map(k -> event.get(k, String.class))
                .collect(Collectors.joining("|"));
        metadata.setAttribute(JOIN_PRIMARY_KEY_METADATA, primaryKeyValue);

        // Child array metadata
        metadata.setAttribute(JOIN_CHILD_TABLE_NAME_METADATA, relation.getChildTable());
        // Child PK name/value: composite joined with |
        final String childPkName = String.join("|", relation.getChildPrimaryKey());
        metadata.setAttribute(JOIN_CHILD_PK_NAME_METADATA, childPkName);
        metadata.setAttribute(JOIN_TYPE_METADATA, relation.getJoinType().getValue());
        metadata.setAttribute(JOIN_MAX_CHILD_RECORDS_METADATA, String.valueOf(relation.getMaxChildRecords()));
        if (!isParent) {
            final String childPkValue = relation.getChildPrimaryKey().stream()
                    .map(k -> event.get(k, String.class))
                    .collect(Collectors.joining("|"));
            metadata.setAttribute(JOIN_CHILD_PK_VALUE_METADATA, childPkValue);
        } else {
            metadata.setAttribute(JOIN_CHILD_PK_VALUE_METADATA, "");
        }
    }

    private JoinRelation getRelation(final String tableName) {
        final JoinRelation relation = parentTableRelations.get(tableName);
        return relation != null ? relation : childTableRelations.get(tableName);
    }

    private Set<String> getExcludeKeys(final JoinRelation relation, final boolean isParent) {
        final Set<String> keys = new HashSet<>();
        keys.addAll(relation.getParentKey());
        keys.addAll(relation.getChildKey());
        if (!isParent) {
            keys.addAll(relation.getChildPrimaryKey());
        }
        return keys;
    }
}
