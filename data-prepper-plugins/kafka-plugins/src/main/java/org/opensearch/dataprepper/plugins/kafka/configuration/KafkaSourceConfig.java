/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.List;


import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSourceConfig {

  @JsonProperty("bootstrap_servers")
  @NotNull
  @Size(min = 1, message = "Bootstrap servers can't be empty")
  private List<String> bootStrapServers;

  @JsonProperty("topics")
  @NotNull
  @Size(min = 1, max = 10, message = "The number of Topics should be between 1 and 10")
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
