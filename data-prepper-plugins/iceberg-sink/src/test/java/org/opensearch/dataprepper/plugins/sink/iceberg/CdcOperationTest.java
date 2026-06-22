/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CdcOperationTest {

    @ParameterizedTest
    @ValueSource(strings = {"index", "insert", "create", "c", "i", "r", "INDEX", "Insert", "CREATE"})
    void from_insert_values(final String value) {
        assertEquals(CdcOperation.INSERT, CdcOperation.from(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"update", "u", "UPDATE", "Update"})
    void from_update_values(final String value) {
        assertEquals(CdcOperation.UPDATE, CdcOperation.from(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"delete", "d", "DELETE", "Delete"})
    void from_delete_values(final String value) {
        assertEquals(CdcOperation.DELETE, CdcOperation.from(value));
    }

    @Test
    void from_null_returns_insert() {
        assertEquals(CdcOperation.INSERT, CdcOperation.from(null));
    }

    @Test
    void from_unknown_returns_null() {
        assertNull(CdcOperation.from("unknown"));
    }

    @Test
    void from_trims_whitespace() {
        assertEquals(CdcOperation.INSERT, CdcOperation.from("  insert  "));
    }
}
