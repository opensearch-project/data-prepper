/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
