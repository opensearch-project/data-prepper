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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enriches CDC events with join metadata so the OpenSearch sink can
 * denormalize related tables into a single document using scripted upsert.
 *
 * Adds the following metadata attributes:
 * <ul>
 *   <li>{@code _table} - source table name</li>
 *   <li>{@code _fields} - comma-separated column names belonging to this table</li>
 *   <li>{@code _is_delete} - "true" if this is a delete event</li>
 *   <li>{@code _primary_key} - the document ID for the denormalized document (parent key value)</li>
 * </ul>
 */
public class JoinMetadataEnricher {

    static final String JOIN_TABLE_METADATA = "_table";
    static final String JOIN_FIELDS_METADATA = "_fields";
    static final String JOIN_IS_DELETE_METADATA = "_is_delete";
    static final String JOIN_PRIMARY_KEY_METADATA = "_primary_key";

    /** tableName -> JoinRelation */
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

    public void enrich(final Event event, final String tableName, final List<String> columnNames, final boolean isDelete) {
        final JoinRelation relation = getRelation(tableName);
        if (relation == null) {
            return;
        }

        final EventMetadata metadata = event.getMetadata();
        metadata.setAttribute(JOIN_TABLE_METADATA, tableName);
        metadata.setAttribute(JOIN_FIELDS_METADATA, String.join(",", columnNames));
        metadata.setAttribute(JOIN_IS_DELETE_METADATA, String.valueOf(isDelete));

        final String primaryKeyValue = resolvePrimaryKey(event, tableName, relation);
        metadata.setAttribute(JOIN_PRIMARY_KEY_METADATA, primaryKeyValue);
    }

    private JoinRelation getRelation(final String tableName) {
        final JoinRelation relation = parentTableRelations.get(tableName);
        return relation != null ? relation : childTableRelations.get(tableName);
    }

    private String resolvePrimaryKey(final Event event, final String tableName, final JoinRelation relation) {
        // For parent table, the document ID is the parent key value
        // For child table, the document ID is the foreign key value (which references the parent key)
        final String keyField = tableName.equals(relation.getParentTable())
                ? relation.getParentKey()
                : relation.getChildKey();
        return event.get(keyField, String.class);
    }
}
