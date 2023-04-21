/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

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
     * Sets custom shouldConclude function
     *
     * @param customShouldConclude function doing custom check
     * @since 2.2
     */
    default void setCustomShouldConclude(Function<Duration, Boolean> customShouldConclude) {
    }
}
