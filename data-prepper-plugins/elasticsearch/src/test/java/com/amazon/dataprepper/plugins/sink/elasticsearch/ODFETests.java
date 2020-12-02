package com.amazon.dataprepper.plugins.sink.elasticsearch;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ODFETests {
    @Test
    public void testODFEConnection() throws IOException {
        final String host = System.getProperty("odfe.host");
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList(host));
        final String user = System.getProperty("odfe.user");
        final String password = System.getProperty("odfe.password");
        if (user != null) {
            builder.withUsername(user);
            builder.withPassword(password);
        }
        final RestHighLevelClient client = builder.build().createClient();
        final ClusterHealthRequest request = new ClusterHealthRequest().waitForYellowStatus();
        final ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        assertEquals(RestStatus.OK, response.status());
    }
}
