/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import java.util.List;


import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicsConfig;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSourceConfig {

  @JsonProperty("bootstrap_servers")
  @NotNull
  private List<String> bootStrapServers;

  @JsonProperty("topics")
  @NotNull
  private List<TopicsConfig> topics;

  public List<String> getBootStrapServers() {
    return bootStrapServers;
  }

  public void setBootStrapServers(List<String> bootStrapServers) {
    this.bootStrapServers = bootStrapServers;
  }
  public List<TopicsConfig> getTopics() {
    return topics;
  }

  public void setTopics(List<TopicsConfig> topics) {
    this.topics = topics;
  }

}
