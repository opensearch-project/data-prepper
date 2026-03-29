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
import java.util.List;

/**
 * Reads shuffle records for a specific partition range from a data file + index file.
 */
public interface ShuffleReader extends Closeable {

    /**
     * Returns the partition offset array from the index file.
     */
    long[] readIndex();

    /**
     * Reads all records belonging to the specified partition range.
     */
    List<ShuffleRecord> readPartitions(int startPartition, int endPartitionInclusive);

    /**
     * Returns raw bytes for the specified byte range of the data file.
     * Used by the HTTP endpoint to serve data to remote readers.
     */
    byte[] readBytes(long offset, int length);
}
