/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import java.util.Optional;

/**
 * A Partition Interface represents an item in the coordination store.
 */
public interface EnhancedPartition<T> {


    String getPartitionType();

    String getPartitionKey();

    Optional<T> getProgressState();

}
