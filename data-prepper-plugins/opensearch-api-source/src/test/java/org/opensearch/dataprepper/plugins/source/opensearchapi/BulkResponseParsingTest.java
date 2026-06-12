/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import org.junit.jupiter.api.Test;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that validate the OpenSearch RestHighLevelClient (used by Flink connector)
 * can successfully parse the empty-items bulk response format.
 *
 * This is the critical compatibility test: if fromXContent doesn't throw,
 * Flink won't crash.
 */
class BulkResponseParsingTest {

    @Test
    void testEmptyItemsSuccessResponse() throws Exception {
        String json = "{\"took\":5,\"errors\":false,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        assertFalse(response.hasFailures());
        assertEquals(0, response.getItems().length);
        assertEquals(5, response.getTook().millis());
    }

    @Test
    void testEmptyItemsErrorResponse() throws Exception {
        String json = "{\"took\":12,\"errors\":true,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        // errors=true but items is empty, so hasFailures checks items array
        // This tests the edge case where errors flag is true but no items detail the error
        assertEquals(0, response.getItems().length);
        assertEquals(12, response.getTook().millis());
    }

    @Test
    void testZeroTookValue() throws Exception {
        String json = "{\"took\":0,\"errors\":false,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        assertFalse(response.hasFailures());
        assertEquals(0, response.getItems().length);
    }

    @Test
    void testLargeTookValue() throws Exception {
        String json = "{\"took\":30000,\"errors\":false,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        assertFalse(response.hasFailures());
        assertEquals(30000, response.getTook().millis());
    }
}
