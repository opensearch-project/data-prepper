package com.amazon.situp.plugins.sink.elasticsearch;

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
        final String user = System.getProperty("odfe.user");
        final String password = System.getProperty("odfe.password");
        final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(
                Collections.singletonList(host)).withUsername(user).withPassword(password).build();
        final RestHighLevelClient client = connectionConfiguration.createClient();
        final ClusterHealthRequest request = new ClusterHealthRequest().waitForYellowStatus();
        final ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        assertEquals(RestStatus.OK, response.status());
    }
}
