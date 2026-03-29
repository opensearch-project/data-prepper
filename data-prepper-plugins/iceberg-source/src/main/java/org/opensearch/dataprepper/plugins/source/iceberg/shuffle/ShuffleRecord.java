/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

/**
 * A single record in the shuffle intermediate data.
 */
public class ShuffleRecord {
    public static final byte OP_DELETE = 0;
    public static final byte OP_INSERT = 1;

    private final byte operation;
    private final int changeOrdinal;
    private final byte[] serializedRecord;

    public ShuffleRecord(final byte operation, final int changeOrdinal, final byte[] serializedRecord) {
        this.operation = operation;
        this.changeOrdinal = changeOrdinal;
        this.serializedRecord = serializedRecord;
    }

    public byte getOperation() {
        return operation;
    }

    public int getChangeOrdinal() {
        return changeOrdinal;
    }

    public byte[] getSerializedRecord() {
        return serializedRecord;
    }
}
