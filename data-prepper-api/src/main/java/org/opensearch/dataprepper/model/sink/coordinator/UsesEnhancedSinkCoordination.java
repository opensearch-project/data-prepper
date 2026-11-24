/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.sink.coordinator;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.function.Function;

/**
 * Sink plugins that need lease-based coordination (e.g., leader election for
 * commit scheduling) implement this interface to receive an
 * {@link EnhancedSourceCoordinator} instance from the pipeline framework.
 */
public interface UsesEnhancedSinkCoordination {

    void setEnhancedSourceCoordinator(final EnhancedSourceCoordinator coordinator);

    Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory();
}
