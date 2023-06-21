/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.exceptions;


import org.opensearch.dataprepper.model.annotations.SkipTestCoverageGenerated;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;

/**
 * This exception is thrown by a {@link org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator} when the source makes a call to perform an action on a
 * partition, but that partition could not be found in the distributed store. The recommended approach to handling this exception is to get a new partition from
 * {@link SourceCoordinator#getNextPartition(java.util.function.Function)}
 */
@SkipTestCoverageGenerated
public class PartitionNotFoundException extends RuntimeException {
    public PartitionNotFoundException(final String message) {
        super(message);
    }
}
