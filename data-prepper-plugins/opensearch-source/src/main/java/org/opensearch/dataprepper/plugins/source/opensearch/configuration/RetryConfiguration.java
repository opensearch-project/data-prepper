/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RetryConfiguration {

  @JsonProperty("max_retries")
  private Integer maxRetries;

  public Integer getMaxRetries() {
    return maxRetries;
  }
}
