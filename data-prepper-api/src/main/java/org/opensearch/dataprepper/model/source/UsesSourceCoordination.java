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
public interface UsesSourceCoordination {
    /**
     *
     * @param sourceCoordinator - The {@link org.opensearch.dataprepper.model.source.SourceCoordinator} to be used by the
     *                          {@link org.opensearch.dataprepper.model.source.Source} as needed
     * @param <T> The partition state type
     */
    <T> void setSourceCoordinator(final SourceCoordinator<T> sourceCoordinator);

    /**
     * @return The model class that will represent the partition progress state. This type should be de/serializable to and from a String
     */
    Class<?> getPartitionProgressStateClass();
}
