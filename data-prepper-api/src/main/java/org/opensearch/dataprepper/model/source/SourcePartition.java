/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import java.util.Objects;

/**
 * The class that will be provided to {@link org.opensearch.dataprepper.model.source.Source} plugins
 * that implement {@link UsesSourceCoordination} to identify the partition of
 * data that the source should process
 * @since 2.2
 */
public class SourcePartition<T> {

    private final PartitionIdentifier partitionIdentifier;
    private final T partitionState;

    private SourcePartition(final Builder<T> builder) {
        Objects.requireNonNull(builder.partitionIdentifier);

        this.partitionIdentifier = builder.partitionIdentifier;
        this.partitionState = builder.partitionState;
    }

    public PartitionIdentifier getPartition() {
        return partitionIdentifier;
    }

    public T getPartitionState() {
        return partitionState;
    }

    public static <T> Builder<T> builder(Class<T> clazz) {
        return new Builder<>(clazz);
    }

    public static class Builder<T> {

        private PartitionIdentifier partitionIdentifier;
        private T partitionState;

        public Builder(Class<T> clazz) {

        }

        public Builder<T> withPartition(final PartitionIdentifier partitionIdentifier) {
            this.partitionIdentifier = partitionIdentifier;
            return this;
        }

        public Builder<T> withPartitionState(final T partitionState) {
            this.partitionState = partitionState;
            return this;
        }

        public SourcePartition<T> build() {
            return new SourcePartition<T>(this);
        }
    }
}
