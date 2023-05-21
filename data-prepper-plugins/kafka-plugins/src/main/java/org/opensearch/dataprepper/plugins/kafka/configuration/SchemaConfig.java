/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class SchemaConfig {

  @JsonProperty("registry_url")
  private String registryURL;

  @JsonProperty("version")
  private int version;

  public String getRegistryURL() {
    return registryURL;
  }

  public void setRegistryURL(String registryURL) {
    this.registryURL = registryURL;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
