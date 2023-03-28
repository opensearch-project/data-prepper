/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination.exceptions;

import org.opensearch.dataprepper.model.source.SourceCoordinator;

/**
 * This exception is thrown when a call to {@link org.opensearch.dataprepper.model.source.SourceCoordinator} is made by a source for a partition
 * that is not owned by this instance of data prepper. If this exception gets thrown, the recommended approach is to call {@link SourceCoordinator#getNextPartition()}
 * to grab a new partition to process
 * @since 2.2
 */
public class PartitionNotOwnedException extends RuntimeException {
    public PartitionNotOwnedException(final String message) {
        super(message);
    }
}
