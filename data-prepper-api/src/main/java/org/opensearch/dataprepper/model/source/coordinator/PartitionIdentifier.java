/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import java.util.Objects;

/**
 * The model class to be passed to {@link SourceCoordinator#getNextPartition(java.util.function.Function)}.
 * The partitionKey should uniquely identify the partition
 * @since 2.2
 */
public class PartitionIdentifier {
    private final String partitionKey;

    private PartitionIdentifier(final PartitionIdentifier.Builder builder) {
        Objects.requireNonNull(builder.partitionKey);

        this.partitionKey = builder.partitionKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public static PartitionIdentifier.Builder builder() {
        return new PartitionIdentifier.Builder();
    }

    public static class Builder {

        private String partitionKey;

        public PartitionIdentifier.Builder withPartitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public PartitionIdentifier build() {
            return new PartitionIdentifier(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionIdentifier that = (PartitionIdentifier) o;
        return Objects.equals(partitionKey, that.partitionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKey);
    }
}
