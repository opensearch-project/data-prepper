/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class OpenSearchIT {
    @Test
    public void testOpenSearchConnection() throws IOException {
        final String host = System.getProperty("tests.opensearch.host");
        final String hostUrl = "https://" + host;
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList(hostUrl));
        final String user = System.getProperty("tests.opensearch.user");
        final String password = System.getProperty("tests.opensearch.password");
        if (user != null) {
            builder.withUsername(user);
            builder.withPassword(password);
        }
        builder.withInsecure(true);
        final AwsCredentialsSupplier awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        final RestHighLevelClient client = builder.build().createClient(awsCredentialsSupplier);

        // TODO: Even with the REST High Level client in OpenSearch 1.1, the standard API
        // does not work with ODFE
        // https://github.com/opensearch-project/OpenSearch/issues/922
        final Request request = new Request("GET", "_cluster/health");
        request.addParameter("master_timeout", "30s");
        request.addParameter("level", "cluster");
        request.addParameter("timeout", "30s");
        request.addParameter("wait_for_status", "yellow");

        final Response response = client.getLowLevelClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }
}
