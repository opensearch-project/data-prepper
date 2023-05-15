/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.exceptions;

import org.opensearch.dataprepper.model.annotations.SkipTestCoverageGenerated;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;

/**
 * This exception is thrown by all other {@link org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator} methods when
 * the {@link SourceCoordinator#initialize()} has not been called yet
 */
@SkipTestCoverageGenerated
public class UninitializedSourceCoordinatorException extends RuntimeException {
    public UninitializedSourceCoordinatorException(final String message) {
        super(message);
    }
}
