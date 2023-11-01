/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

public interface InternalEventHandle {
    /**
     * sets acknowledgement set
     *
     * @param acknowledgementSet acknowledgementSet to be set in the event handle
     * @since 2.6
     */
    void setAcknowledgementSet(final AcknowledgementSet acknowledgementSet);

    /**
     * gets acknowledgement set
     *
     * @return returns acknowledgementSet from the event handle
     * @since 2.6
     */
    AcknowledgementSet getAcknowledgementSet();

}

