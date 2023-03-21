/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import java.util.Objects;

/**
 * The class that will be provided to {@link org.opensearch.dataprepper.model.source.Source} plugins
 * that implement {@link org.opensearch.dataprepper.model.source.RequiresSourceCoordination} to identify the partition of
 * data that the source should process
 * @since 2.2
 */
public class SourcePartition {

    private final String partitionKey;
    private final Object partitionState;

    private SourcePartition(final Builder builder) {
        Objects.requireNonNull(builder.partitionKey);

        this.partitionKey = builder.partitionKey;
        this.partitionState = builder.partitionState;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public Object getPartitionState() {
        return partitionState;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String partitionKey;
        private Object partitionState;

        public Builder withPartitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder withPartitionState(final Object partitionState) {
            this.partitionState = partitionState;
            return this;
        }

        public SourcePartition build() {
            return new SourcePartition(this);
        }
    }
}
