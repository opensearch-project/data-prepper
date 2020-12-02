package com.amazon.dataprepper.plugins.sink.elasticsearch;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectionPipelineComponentIT extends ESRestTestCase {
  public static List<String> HOSTS = Arrays.stream(System.getProperty("tests.rest.cluster").split(","))
          .map(ip -> "http://" + ip).collect(Collectors.toList());

  public void testCreateClientSimple() throws IOException {
    final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(HOSTS)
            .withUsername("")
            .withPassword("")
            .build();
    final RestHighLevelClient client = connectionConfiguration.createClient();
    assertNotNull(client);
    client.close();
  }

  public void testCreateClientTimeout() throws IOException {
    final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(HOSTS)
            .withConnectTimeout(5)
            .withSocketTimeout(10)
            .build();
    final RestHighLevelClient client = connectionConfiguration.createClient();
    assertNotNull(client);
    client.close();
  }
}
