/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

