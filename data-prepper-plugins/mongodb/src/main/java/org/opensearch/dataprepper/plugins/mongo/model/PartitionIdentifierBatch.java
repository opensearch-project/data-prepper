/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.model;

import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;

import java.util.List;

public class PartitionIdentifierBatch {
    private final List<PartitionIdentifier> partitionIdentifiers;

    private final boolean isLastBatch;
    private final Object endDocId;

    public PartitionIdentifierBatch(final List<PartitionIdentifier> partitionIdentifiers,
                                    final boolean isLastBatch,
                                    final Object endDocId) {
        this.partitionIdentifiers = partitionIdentifiers;
        this.isLastBatch = isLastBatch;
        this.endDocId = endDocId;
    }

    public List<PartitionIdentifier> getPartitionIdentifiers() {
        return partitionIdentifiers;
    }

    public boolean isLastBatch() {
        return isLastBatch;
    }

    public Object getEndDocId() {
        return endDocId;
    }
}
