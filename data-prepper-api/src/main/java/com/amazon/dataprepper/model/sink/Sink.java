/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.sink;

import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Data Prepper sink interface. Sink may publish records to a disk, a file,
 * to OpenSearch, other pipelines, or other external systems.
 */
public interface Sink<T extends Record<?>> {

    /**
     * outputs collection of records which extend {@link Record}.
     *
     * @param records the records to write to the sink.
     */
    void output(Collection<T> records);

    /**
     * Prepare sink for shutdown, by cleaning up resources and threads.
     */
    void shutdown();

}
