/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CloudwatchPartitionCreationSupplier implements Function<Map<String, Object>, List<PartitionIdentifier>> {

    private List<String> metrics;

    public CloudwatchPartitionCreationSupplier(final List<String> metrics) {
        this.metrics = metrics;
    }

    @Override
    public List<PartitionIdentifier> apply(final Map<String, Object> globalStateMap) {
        final List<PartitionIdentifier> objectsToProcess = new ArrayList<>();
        for ( String metric : metrics) {
            objectsToProcess.add(PartitionIdentifier.builder().withPartitionKey(metric).build());
        }
        return objectsToProcess;
    }
}
