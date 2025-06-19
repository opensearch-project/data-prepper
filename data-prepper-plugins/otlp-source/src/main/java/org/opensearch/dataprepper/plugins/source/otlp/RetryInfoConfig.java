/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otlp;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RetryInfoConfig {

  @JsonProperty(value = "min_delay", defaultValue = "100ms")
  private Duration minDelay;

  @JsonProperty(value = "max_delay", defaultValue = "2s")
  private Duration maxDelay;

  // Jackson needs this constructor
  public RetryInfoConfig() {
  }

  public RetryInfoConfig(Duration minDelay, Duration maxDelay) {
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
  }

  public Duration getMinDelay() {
    return minDelay;
  }

  public void setMinDelay(Duration minDelay) {
    this.minDelay = minDelay;
  }

  public Duration getMaxDelay() {
    return maxDelay;
  }

  public void setMaxDelay(Duration maxDelay) {
    this.maxDelay = maxDelay;
  }
}
