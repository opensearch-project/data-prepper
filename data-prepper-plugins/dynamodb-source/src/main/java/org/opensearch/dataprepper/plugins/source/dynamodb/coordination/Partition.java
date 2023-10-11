/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination;

import java.util.Optional;

/**
 * A Partition Interface represents an item in the coordination store.
 */
public interface Partition<T> {


    String getPartitionType();

    String getPartitionKey();

    Optional<T> getProgressState();

}
