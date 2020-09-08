package com.amazon.situp.model.sink;

import com.amazon.situp.model.record.Record;

import java.util.Collection;

/**
 * SITUP sink interface. Sink may publish records to a disk or a file or
 * to elasticsearch or other pipelines or external systems
 */
public interface Sink<T extends Record<?>> {

    /**
     * outputs collection of records which extend {@link Record}.
     *
     * TODO: rethink boolean output, might be better off as int for number of records written successfully.
     * @param records the records to write to the sink.
     * @return boolean as to the success of the writing.
     */
    boolean output(Collection<T> records);

}
