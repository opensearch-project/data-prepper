/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

public interface InternalEventHandle {
    /**
     * adds acknowledgement set
     *
     * @param acknowledgementSet acknowledgementSet to be set in the event handle
     * @since 2.9
     */
    void addAcknowledgementSet(final AcknowledgementSet acknowledgementSet);

    /**
     * Indicates if the event handle has atleast one acknowledgement set
     *
     * @return returns true if there is at least one acknowledgementSet in the event handle
     * @since 2.9
     */
    boolean hasAcknowledgementSet();

    /**
     * Acquires reference to acknowledgement set(s) in the event handle
     *
     * @since 2.9
     */
    void acquireReference();

}

