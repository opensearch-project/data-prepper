/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model.processor;

import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Processor interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Processor<InputRecord extends Record<?>, OutputRecord extends Record<?>> {

    /**
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    Collection<OutputRecord> execute(Collection<InputRecord> records);

    /**
     * Indicates to the processor that shutdown is imminent and any data currently held by the Processor
     * should be flushed downstream.
     */
    void prepareForShutdown();

    /**
     * Returns true if the Processor's internal state is safe to be shutdown.
     *
     * @return shutdown readiness status
     */
    boolean isReadyForShutdown();

    /**
     * Final shutdown call to clean up any resources that need to be closed.
     */
    void shutdown();
}
