/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import java.util.Optional;

/**
 * A Partition Interface represents an item in the coordination store.
 */
public interface EnhancedPartition<T> {


    String getPartitionType();

    String getPartitionKey();

    Optional<T> getProgressState();

}
