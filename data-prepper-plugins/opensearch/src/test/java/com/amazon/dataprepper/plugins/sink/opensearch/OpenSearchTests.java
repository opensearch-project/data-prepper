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
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

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
        final ClusterHealthRequest request = new ClusterHealthRequest().waitForYellowStatus();
        final ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        assertThat(response.status(), equalTo(RestStatus.OK));
    }
}
