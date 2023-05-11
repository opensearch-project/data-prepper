/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import org.opensearch.dataprepper.model.annotations.SkipTestCoverageGenerated;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;

/**
 * The status representation of a source partition, to be used by {@link SourceCoordinator}
 * and {@link SourceCoordinationStore}
 * @since 2.2
 */
@SkipTestCoverageGenerated
public enum SourcePartitionStatus {
    /**
     * Represents a source partition that is owned by either this instance of data prepper, or another instance of data prepper
     * that is also processing on the same {@link SourceCoordinationStore}
     */
    ASSIGNED,

    /**
     * Represents a source partition that is not owned by any instance of data prepper,
     * and is available to be acquired for processing
     */
    UNASSIGNED,

    /**
     * Represents a source partition that has already been fully processed
     */
    COMPLETED,

    /**
     * Represents a source partition that has been closed for a temporary duration of time. The partition will be considered as UNASSIGNED once the
     * {@link SourcePartitionStoreItem#getReOpenAt()} instant is reached, and will be considered COMPLETED once the {@link SourcePartitionStoreItem#getClosedCount()}
     * reaches the configured limit for number of times the partition has been closed throughout its lifetime
     */
    CLOSED
}
