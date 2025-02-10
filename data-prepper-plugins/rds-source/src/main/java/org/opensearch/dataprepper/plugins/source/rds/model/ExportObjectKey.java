/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Represents the object key for an object exported to S3 by RDS.
 * The object key has this structure: "{prefix}/{export task ID}/{database name}/{full table name}/{numbered folder}/{file name}"
 */
public class ExportObjectKey {

    static final String S3_PATH_DELIMITER = "/";
    private final String prefix;
    private final String exportTaskId;
    private final String databaseName;

    /**
     * schemaName is specific for Postgres; For MySQL, this schemaName has the same value as databaseName
     */
    private final String schemaName;

    private final String tableName;
    private final String numberedFolder;
    private final String fileName;

    ExportObjectKey(final String prefix, final String exportTaskId, final String databaseName, final String schemaName, final String tableName, final String numberedFolder, final String fileName) {
        this.prefix = prefix;
        this.exportTaskId = exportTaskId;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.numberedFolder = numberedFolder;
        this.fileName = fileName;
    }

    public static ExportObjectKey fromString(final String objectKeyString) {

        final String[] parts = objectKeyString.split(S3_PATH_DELIMITER);
        if (parts.length < 5) {
            throw new IllegalArgumentException("Export object key is not valid: " + objectKeyString);
        }

        final String prefix = Arrays.stream(parts, 0, parts.length - 5)
                .collect(Collectors.joining(S3_PATH_DELIMITER));
        final String exportTaskId = parts[parts.length - 5];
        final String databaseName = parts[parts.length - 4];
        // fullTableName is in the format of "databaseName.tableName" for MySQL and "schemaName.tableName" for Postgres
        final String fullTableName = parts[parts.length - 3];
        final String schemaName = fullTableName.split("\\.")[0];
        final String tableName = fullTableName.split("\\.")[1];
        final String numberedFolder = parts[parts.length - 2];
        final String fileName = parts[parts.length - 1];

        return new ExportObjectKey(prefix, exportTaskId, databaseName, schemaName, tableName, numberedFolder, fileName);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getExportTaskId() {
        return exportTaskId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * schemaName is specific for Postgres; For MySQL, this schemaName has the same value as databaseName
     * @return schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getNumberedFolder() {
        return numberedFolder;
    }

    public String getFileName() {
        return fileName;
    }
}
