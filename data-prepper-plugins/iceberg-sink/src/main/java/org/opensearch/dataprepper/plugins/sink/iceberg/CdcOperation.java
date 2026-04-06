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

public enum CdcOperation {
    INSERT,
    UPDATE,
    DELETE;

    public static CdcOperation from(final String value) {
        if (value == null) {
            return INSERT;
        }
        switch (value.trim().toLowerCase()) {
            case "index":
            case "insert":
            case "create":
            case "c":
            case "i":
            case "r":
                return INSERT;
            case "update":
            case "u":
                return UPDATE;
            case "delete":
            case "d":
                return DELETE;
            default:
                return null;
        }
    }
}
