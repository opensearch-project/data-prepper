/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

public interface EventBuilder extends BaseEventBuilder<Event> {
    /**
     * Returns a newly created {@link JacksonEvent}.
     *
     * @return an event
     * @since 2.2
     */
    Event build();
}

