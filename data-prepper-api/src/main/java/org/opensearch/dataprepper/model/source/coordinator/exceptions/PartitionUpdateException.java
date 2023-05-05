/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.exceptions;

import org.opensearch.dataprepper.model.annotations.SkipTestCoverageGenerated;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

/**
 * This exception should be thrown by
 * {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore#tryUpdateSourcePartitionItem(SourcePartitionStoreItem)}
 * when the item cannot be updated for any reason
 */
@SkipTestCoverageGenerated
public class PartitionUpdateException extends RuntimeException {
    public PartitionUpdateException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
