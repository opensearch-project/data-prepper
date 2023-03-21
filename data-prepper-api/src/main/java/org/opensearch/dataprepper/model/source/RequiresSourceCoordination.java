/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

/**
 * An interface that a {@link org.opensearch.dataprepper.model.source.Source} will implement to be provided
 * with a {@link org.opensearch.dataprepper.model.source.SourceCoordinator} when source_coordination is configured
 * @since 2.2
 */
public interface RequiresSourceCoordination {
    /**
     *
     * @param sourceCoordinator - The {@link org.opensearch.dataprepper.model.source.SourceCoordinator} to be used by the
     *                          {@link org.opensearch.dataprepper.model.source.Source} as needed
     */
    void setSourceCoordinator(final SourceCoordinator sourceCoordinator);
}
