/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.Map;
import java.util.function.Function;
import java.time.Duration;

/**
 * Implementing classes are able to be passed to the functions of {@link AggregateAction}
 * @since 1.3
 */
public interface AggregateActionInput {

    /**
     * @return An implementation of {@link GroupState}
     * @since 1.3
     */
    GroupState getGroupState();

    /**
     * @return a map of Identification keys containing identification keys and
     *         their values
     * @since 2.1
     */
    Map<Object, Object> getIdentificationKeys();

    /**
     * @return returns eventHandle held by the instance
     * @since 2.11
     */
    EventHandle getEventHandle();

    /**
     * Sets custom shouldConclude function
     *
     * @param customShouldConclude function doing custom check
     * @since 2.2
     */
    default void setCustomShouldConclude(Function<Duration, Boolean> customShouldConclude) {
    }
}
