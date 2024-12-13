/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.processor;

import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * @since 1.2
 * Processor interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Processor<InputRecord extends Record<?>, OutputRecord extends Record<?>> {

    /**
     * @since 1.2
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    Collection<OutputRecord> execute(Collection<InputRecord> records);

    /**
     * @since 1.2
     * Indicates to the processor that shutdown is imminent and any data currently held by the Processor
     * should be flushed downstream.
     */
    void prepareForShutdown();

    /**
     * @since 2.11
     * Indicates if the processor holds the events or not
     * Holding events indicates that the events are not ready to be released.
     */
    default boolean holdsEvents() {
        return false;
    }

    /**
     * @since 1.2
     * Returns true if the Processor's internal state is safe to be shutdown.
     *
     * @return shutdown readiness status
     */
    boolean isReadyForShutdown();

    /**
     * @since 1.2
     * Final shutdown call to clean up any resources that need to be closed.
     */
    void shutdown();
}
