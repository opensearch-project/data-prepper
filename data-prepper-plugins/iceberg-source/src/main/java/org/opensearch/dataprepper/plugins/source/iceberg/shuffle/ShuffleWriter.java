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

import java.io.Closeable;

/**
 * Writes shuffle records to a single data file + index file, sorted by partition number.
 * <p>
 * Usage:
 * <pre>
 *   writer.addRecord(partitionNumber, operation, changeOrdinal, avroBytes);
 *   // ... add all records ...
 *   writer.finish();  // sorts and writes to disk
 * </pre>
 */
public interface ShuffleWriter extends Closeable {

    void addRecord(int partitionNumber, byte operation, int changeOrdinal, byte[] serializedRecord);

    /**
     * Sorts buffered records by partition number and writes data file + index file.
     */
    void finish();
}
