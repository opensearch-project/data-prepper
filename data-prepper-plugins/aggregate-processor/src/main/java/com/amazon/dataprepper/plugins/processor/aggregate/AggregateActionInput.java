/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

/**
 * Implementing classes are able to be passed to the functions of {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateAction}
 * @since 1.3
 */
public interface AggregateActionInput {

    /**
     * @return An implementation of {@link com.amazon.dataprepper.plugins.processor.aggregate.GroupState}
     * @since 1.3
     */
    GroupState getGroupState();
}
