package com.amazon.dataprepper.model.prepper;

import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Prepper interface. These are intermediary processing units using which users can filter,
 * transform and enrich the records into desired format before publishing to the sink.
 */
public interface Prepper<InputRecord extends Record<?>, OutputRecord extends Record<?>> {

    /**
     * execute the prepper logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    Collection<OutputRecord> execute(Collection<InputRecord> records);

    /**
     * Prepare prepper for shutdown, by cleaning up resources and threads.
     */
    void shutdown();
}
