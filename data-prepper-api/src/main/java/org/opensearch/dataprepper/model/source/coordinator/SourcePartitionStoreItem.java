/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import java.time.Instant;

/**
 * The interface that should be implemented when creating store specific {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore}
 * plugin implementations.
 * @since 2.2
 */
public interface SourcePartitionStoreItem {
    String getSourcePartitionKey();
    String getPartitionOwner();
    String getPartitionProgressState();
    SourcePartitionStatus getSourcePartitionStatus();
    Instant getPartitionOwnershipTimeout();
    Instant getReOpenAt();
    Long getClosedCount();
}
