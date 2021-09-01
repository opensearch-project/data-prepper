/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class OpenSearchTests {
    @Test
    public void testOpenSearchConnection() throws IOException {
        final String host = System.getProperty("os.host");
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList(host));
        final String user = System.getProperty("os.user");
        final String password = System.getProperty("os.password");
        if (user != null) {
            builder.withUsername(user);
            builder.withPassword(password);
        }
        final RestHighLevelClient client = builder.build().createClient();

        // TODO: Use the REST High Level client in OpenSearch 1.0.1 or 1.1, where this will work.
        // https://github.com/opensearch-project/OpenSearch/issues/922
        final Request request = new Request("GET", "_cluster/health");
        request.addParameter("master_timeout", "30s");
        request.addParameter("level", "cluster");
        request.addParameter("timeout", "30s");
        request.addParameter("wait_for_status", "yellow");

        final Response response = client.getLowLevelClient().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }
}
