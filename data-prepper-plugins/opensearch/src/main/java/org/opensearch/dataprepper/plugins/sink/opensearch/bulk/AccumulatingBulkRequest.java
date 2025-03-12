/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import java.util.List;

/**
 * Accumulates Bulk Requests.
 *
 * @param <O> OperationType
 * @param <R> RequestType
 */
public interface AccumulatingBulkRequest<O, R> {
    long estimateSizeInBytesWithDocument(O documentOrOperation);

    void addOperation(O documentOrOperation);

    O getOperationAt(int index);

    long getEstimatedSizeInBytes();

    int getOperationsCount();

    List<O> getOperations();

    R getRequest();
}
