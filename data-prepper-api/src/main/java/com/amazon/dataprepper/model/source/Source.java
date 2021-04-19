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

package com.amazon.dataprepper.model.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;

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
}
