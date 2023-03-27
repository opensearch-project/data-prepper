/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

public interface EventFactory {
    /**
     * Builds events of a given class
     *
     * @param eventBuilderClass class of the event builder object
     * @param <T> The event type
     * @param <B> The base event builder type
     * @return Returns builder of type B
     * @since 2.2
     */
    <T extends Event, B extends BaseEventBuilder<T>> B eventBuilder(Class<B> eventBuilderClass);
}
