/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import java.util.Objects;
import java.util.Optional;

/**
 * The class that will be provided to {@link org.opensearch.dataprepper.model.source.Source} plugins
 * that implement {@link UsesSourceCoordination} to identify the partition of
 * data that the source should process. This is returned in a call to {@link SourceCoordinator#getNextPartition(java.util.function.Function)}.
 * @since 2.2
 */
public class SourcePartition<T> {

    private final String partitionKey;
    private final T partitionState;
    private final Long partitionClosedCount;

    private SourcePartition(final Builder<T> builder) {
        Objects.requireNonNull(builder.partitionKey);

        this.partitionKey = builder.partitionKey;
        this.partitionState = builder.partitionState;
        this.partitionClosedCount = builder.partitionClosedCount;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public Optional<T> getPartitionState() {
        return Optional.ofNullable(partitionState);
    }

    public Long getPartitionClosedCount() {
        return partitionClosedCount;
    }

    public static <T> Builder<T> builder(Class<T> clazz) {
        return new Builder<>(clazz);
    }

    public static class Builder<T> {

        private String partitionKey;
        private T partitionState;
        private Long partitionClosedCount;

        public Builder(Class<T> clazz) {

        }

        public Builder<T> withPartitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder<T> withPartitionState(final T partitionState) {
            this.partitionState = partitionState;
            return this;
        }

        public Builder<T> withPartitionClosedCount(final Long partitionClosedCount) {
            this.partitionClosedCount = partitionClosedCount;
            return this;
        }

        public SourcePartition<T> build() {
            return new SourcePartition<T>(this);
        }
    }
}
