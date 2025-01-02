/*
 *
 * Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.neptune.client;

import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamPosition;

import java.util.List;

public interface NeptuneStreamEventListener {
    /**
     * @param records the records returned from the Stream.
     * @param streamPosition current commitNum and OpNum in the stream.
     */
    void onNeptuneStreamEvents(final List<NeptuneStreamRecord> records, final StreamPosition streamPosition);

    /**
     *
     * @param exception any encountered exception during stream processing
     * @param streamPosition current commitNum and OpNum in the stream
     * @return boolean if the execution should continue after that exception is encountered.
     */
    boolean onNeptuneStreamException(final Exception exception, final StreamPosition streamPosition);

    /**
     *
     * @param streamPosition current commitNum and OpNum in the stream
     * @return boolean if stream processing should be stopped
     */
    boolean shouldStopNeptuneStream(final StreamPosition streamPosition);
}
