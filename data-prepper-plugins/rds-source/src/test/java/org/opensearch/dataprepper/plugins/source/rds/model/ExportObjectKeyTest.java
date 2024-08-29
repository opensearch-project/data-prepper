/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExportObjectKeyTest {

    @Test
    void test_fromString_with_valid_input_string() {
        final String objectKeyString = "prefix/export-task-id/db-name/db-name.table-name/1/file-name.parquet";
        final ExportObjectKey exportObjectKey = ExportObjectKey.fromString(objectKeyString);

        assertThat(exportObjectKey.getPrefix(), equalTo("prefix"));
        assertThat(exportObjectKey.getExportTaskId(), equalTo("export-task-id"));
        assertThat(exportObjectKey.getDatabaseName(), equalTo("db-name"));
        assertThat(exportObjectKey.getTableName(), equalTo("table-name"));
        assertThat(exportObjectKey.getNumberedFolder(), equalTo("1"));
        assertThat(exportObjectKey.getFileName(), equalTo("file-name.parquet"));
    }

    @Test
    void test_fromString_with_invalid_input_string() {
        final String objectKeyString = "prefix/export-task-id/db-name/table-name/1/";

        Throwable exception = assertThrows(IllegalArgumentException.class, () -> ExportObjectKey.fromString(objectKeyString));
        assertThat(exception.getMessage(), containsString("Export object key is not valid: " + objectKeyString));
    }
}