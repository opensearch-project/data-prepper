/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

/**
 * A factory for producing {@link EventKey} objects.
 *
 * @since 2.9
 */
public interface EventKeyFactory {
    /**
     * Creates an {@link EventKey} with given actions.
     *
     * @param key The key
     * @param forActions Actions to support
     * @return The EventKey
     * @since 2.9
     */
    EventKey createEventKey(String key, EventAction... forActions);

    /**
     * Creates an {@link EventKey} for the default actions, which are all.
     *
     * @param key The key
     * @return The EventKey
     * @since 2.9
     */
    default EventKey createEventKey(final String key) {
        return createEventKey(key, EventAction.ALL);
    }

    /**
     * An action on an Event.
     *
     * @since 2.9
     */
    enum EventAction {
        GET,
        DELETE,
        PUT,
        ALL;

        boolean isMutableAction() {
            return this != GET;
        }

        boolean supports(final EventAction eventAction) {
            if(this == ALL)
                return true;
            return this == eventAction;
        }
    }
}
