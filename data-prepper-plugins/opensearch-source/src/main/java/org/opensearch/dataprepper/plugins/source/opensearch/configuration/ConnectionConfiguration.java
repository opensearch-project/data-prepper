/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;
import java.time.Duration;

public class ConnectionConfiguration {

  @JsonProperty("cert")
  private Path certPath;

  @JsonProperty("socket_timeout")
  private Duration socketTimeout;

  @JsonProperty("connection_timeout")
  private Duration connectTimeout;

  @JsonProperty("insecure")
  private boolean insecure = false;

  public Path getCertPath() {
    return certPath;
  }

  public Duration getSocketTimeout() {
    return socketTimeout;
  }

  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  public boolean isInsecure() {
    return insecure;
  }
}
