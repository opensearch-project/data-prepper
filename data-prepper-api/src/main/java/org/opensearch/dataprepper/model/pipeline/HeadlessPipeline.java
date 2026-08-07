/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.pipeline;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

public interface HeadlessPipeline {
    /**
     * sets flag to indicate if acknowledgements are enabled
     *
     * @param acknowledgementsEnabled flag indicating acknowledgements are enabled
     * @since 2.13
     */
    void setAcknowledgementsEnabled(final boolean acknowledgementsEnabled);

    /**
     * sends events to the headless pipeline
     *
     * @param events records to be sent to headless pipeline
     * @since 2.13
     */
    void sendEvents(Collection<Record<Event>> events);
    
}
