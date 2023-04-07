/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class BulkOperationWithHandleTests {
    private BulkOperation bulkOperation;

    BulkOperationWithHandle createObjectUnderTest(EventHandle eventHandle) {
        bulkOperation = mock(BulkOperation.class);
        if (eventHandle == null) {
            return new BulkOperationWithHandle(bulkOperation);
        }
        return new BulkOperationWithHandle(bulkOperation, eventHandle);
    }

    @Test
    public void testConstructorWithOneArgument() {
        BulkOperationWithHandle bulkOperationWithHandle = createObjectUnderTest(null);
        assertThat(bulkOperationWithHandle.getBulkOperation(), equalTo(bulkOperation));
        assertThat(bulkOperationWithHandle.getEventHandle(), equalTo(null));
    }

    @Test
    public void testConstructorWithTwoArguments() {
        EventHandle eventHandle = mock(EventHandle.class);
        BulkOperationWithHandle bulkOperationWithHandle = createObjectUnderTest(eventHandle);
        assertThat(bulkOperationWithHandle.getBulkOperation(), equalTo(bulkOperation));
        assertThat(bulkOperationWithHandle.getEventHandle(), equalTo(eventHandle));
    }
}
