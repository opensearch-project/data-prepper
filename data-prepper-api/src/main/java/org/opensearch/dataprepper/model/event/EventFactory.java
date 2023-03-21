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
     * @since 2.2
     */
    <T extends Event, B extends BaseEventBuilder<T>> B eventBuilder(Class<B> eventBuilderClass);
}
