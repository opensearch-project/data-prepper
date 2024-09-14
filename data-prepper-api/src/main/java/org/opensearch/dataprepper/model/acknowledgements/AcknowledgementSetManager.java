/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.acknowledgements;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * AcknowledgementSetManager manages acknowledgement sets created by
 * source(s). It facilitates creation of acknowledgement set and
 * allows references for events in the acknowledgement sets to be
 * acquired or released and when the final event in an acknowledgement
 * set is released, the registered callback is invoked.
 */
public interface AcknowledgementSetManager {
    /**
     * Creates an acknowledgement set
     *
     * @param callback callback function to be invoked
     * @param timeout expiry timeout
     *
     * @return AcknowledgementSet returns a new acknowledgement set
     * @since 2.2
     */
    AcknowledgementSet create(final Consumer<Boolean> callback, final Duration timeout);
}
