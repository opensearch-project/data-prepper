/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TopicsConfig {
  @JsonProperty("topic")
  private TopicConfig topic;

  public TopicConfig getTopic() {
	return topic;
  }

  public TopicsConfig setTopics(TopicConfig topic) {
	this.topic = topic;
	return this;
  }
}
