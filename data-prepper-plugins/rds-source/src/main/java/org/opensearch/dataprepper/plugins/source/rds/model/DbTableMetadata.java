/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Map;

public class DbTableMetadata {

    private static final String DB_METADATA_KEY = "dbMetadata";
    private static final String TABLE_COLUMN_METADATA_KEY = "tableColumnDataTypeMap";
    private final DbMetadata dbMetadata;
    private final Map<String, Map<String, String>> tableColumnDataTypeMap;

    public DbTableMetadata(final DbMetadata dbMetadata, final Map<String, Map<String, String>> tableColumnDataTypeMap) {
        this.dbMetadata = dbMetadata;
        this.tableColumnDataTypeMap = tableColumnDataTypeMap;
    }
    
    public DbMetadata getDbMetadata() {
        return dbMetadata;
    }

    public Map<String, Map<String, String>> getTableColumnDataTypeMap() {
        return tableColumnDataTypeMap;
    }
    
    public Map<String, Object> toMap() {
        return Map.of(
                DB_METADATA_KEY, dbMetadata.toMap(),
                TABLE_COLUMN_METADATA_KEY, tableColumnDataTypeMap
        );
    }

    public static DbTableMetadata fromMap(Map<String, Object> map) {
        return new DbTableMetadata(
                DbMetadata.fromMap((Map<String, Object>)map.get(DB_METADATA_KEY)),
                (Map<String, Map<String, String>>) map.get(TABLE_COLUMN_METADATA_KEY)
        );
    }
}
