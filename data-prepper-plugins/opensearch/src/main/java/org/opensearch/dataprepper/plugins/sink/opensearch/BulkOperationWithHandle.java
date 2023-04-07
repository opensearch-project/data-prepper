/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.dataprepper.model.event.EventHandle;

public class BulkOperationWithHandle {
    private EventHandle eventHandle;
    private BulkOperation bulkOperation;

    public BulkOperationWithHandle(final BulkOperation bulkOperation) {
        this.bulkOperation = bulkOperation;
        this.eventHandle = null;
    }

    public BulkOperationWithHandle(final BulkOperation bulkOperation, EventHandle eventHandle) {
        this.bulkOperation = bulkOperation;
        this.eventHandle = eventHandle;
    }

    public BulkOperation getBulkOperation() {
        return bulkOperation;
    }

    public EventHandle getEventHandle() {
        return eventHandle;
    }
}
