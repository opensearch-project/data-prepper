package com.amazon.dataprepper.model.processor;

import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Data Prepper Processor interface. These are intermediary processing units using which users can filter,
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
}
