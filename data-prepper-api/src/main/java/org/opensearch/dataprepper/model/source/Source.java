/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

/**
 * Data Prepper source interface. Source acts as receiver of the events that flow
 * through the transformation pipeline.
 */
public interface Source<T extends Record<?>> {

    /**
     * Notifies the source to start writing the records into the buffer
     *
     * @param buffer Buffer to which the records will be queued or written to.
     */
    void start(Buffer<T> buffer);

    /**
     * Notifies the source to stop consuming/writing records into Buffer.
     */
    void stop();

    /**
     * Indicates if the source has end to end acknowledgements enabled
     *
     * @return boolean indicating if the source enabled end-to-end acknowledgements
     * @since 2.2
     */
    default boolean areAcknowledgementsEnabled() {
        return false;
    }
}
