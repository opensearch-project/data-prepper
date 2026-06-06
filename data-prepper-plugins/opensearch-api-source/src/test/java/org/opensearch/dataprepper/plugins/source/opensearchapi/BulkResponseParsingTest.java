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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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

        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems().length, equalTo(0));
        assertThat(response.getTook().millis(), equalTo(5L));
    }

    @Test
    void testEmptyItemsErrorResponse() throws Exception {
        String json = "{\"took\":12,\"errors\":true,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        assertThat(response.getItems().length, equalTo(0));
        assertThat(response.getTook().millis(), equalTo(12L));
    }

    @Test
    void testZeroTookValue() throws Exception {
        String json = "{\"took\":0,\"errors\":false,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems().length, equalTo(0));
    }

    @Test
    void testLargeTookValue() throws Exception {
        String json = "{\"took\":30000,\"errors\":false,\"items\":[]}";

        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        BulkResponse response = BulkResponse.fromXContent(parser);

        assertThat(response.hasFailures(), is(false));
        assertThat(response.getTook().millis(), equalTo(30000L));
    }
}
