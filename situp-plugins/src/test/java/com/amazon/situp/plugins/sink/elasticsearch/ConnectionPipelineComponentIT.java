package com.amazon.situp.plugins.sink.elasticsearch;

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
    ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(HOSTS)
            .withUsername("")
            .withPassword("")
            .build();
    RestHighLevelClient client = connectionConfiguration.createClient();
    client.close();
  }
}
