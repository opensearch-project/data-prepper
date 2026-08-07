/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.util.function.Function;

public interface UsesEnhancedSourceCoordination {

    /**
     *
     * @param sourceCoordinator - The {@link EnhancedSourceCoordinator} to be used by the
     *                          {@link org.opensearch.dataprepper.model.source.Source} as needed
     */
    void setEnhancedSourceCoordinator(final EnhancedSourceCoordinator sourceCoordinator);

    Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory();
}
