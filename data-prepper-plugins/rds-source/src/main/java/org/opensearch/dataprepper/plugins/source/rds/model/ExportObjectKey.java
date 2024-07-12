/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

/**
 * Represents the object key for an object exported to S3 by RDS.
 * The object key has this structure: "{prefix}/{export task ID}/{database name}/{table name}/{numbered folder}/{file name}"
 */
public class ExportObjectKey {

    private final String prefix;
    private final String exportTaskId;
    private final String databaseName;
    private final String tableName;
    private final String numberedFolder;
    private final String fileName;

    ExportObjectKey(final String prefix, final String exportTaskId, final String databaseName, final String tableName, final String numberedFolder, final String fileName) {
        this.prefix = prefix;
        this.exportTaskId = exportTaskId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.numberedFolder = numberedFolder;
        this.fileName = fileName;
    }

    public static ExportObjectKey fromString(final String objectKeyString) {

        final String[] parts = objectKeyString.split("/");
        if (parts.length != 6) {
            throw new IllegalArgumentException("Export object key is not valid: " + objectKeyString);
        }
        final String prefix = parts[0];
        final String exportTaskId = parts[1];
        final String databaseName = parts[2];
        final String tableName = parts[3];
        final String numberedFolder = parts[4];
        final String fileName = parts[5];
        return new ExportObjectKey(prefix, exportTaskId, databaseName, tableName, numberedFolder, fileName);
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
