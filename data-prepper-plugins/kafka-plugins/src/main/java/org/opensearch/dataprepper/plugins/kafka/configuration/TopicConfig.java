/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class TopicConfig {

  @JsonProperty("name")
  @NotNull
  @Valid
  private String topic;

  @JsonProperty("consumer")
  @NotNull
  @Valid
  private ConsumerConfigs consumerConfigs;

  @JsonProperty("auth_type")
  @NotNull
  @Valid
  private String authType;

  @JsonProperty("schema")
  @NotNull
  @Valid
  private SchemaConfig schemaConfig;
  public String getName() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public ConsumerConfigs getConsumerGroupConfig() {
	return consumerConfigs;
  }

  public void setConsumerGroupConfig(ConsumerConfigs consumerConfigs) {
	this.consumerConfigs = consumerConfigs;
  }

  public String getAuthType() {
	return authType;
  }

  public void setAuthType(String authType) {
	this.authType = authType;
  }

  public SchemaConfig getSchemaConfig() {
	return schemaConfig;
  }
   public void setSchemaConfig(SchemaConfig schemaConfig) {
	this.schemaConfig = schemaConfig;
  }
}
