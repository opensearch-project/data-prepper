/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;

public class ConnectionConfiguration {

  @JsonProperty("cert")
  private Path certPath;

  @JsonProperty("socket_timeout")
  private Integer socketTimeout;

  @JsonProperty("connection_timeout")
  private Integer connectTimeout;

  @JsonProperty("insecure")
  private boolean insecure;

  public Path getCertPath() {
    return certPath;
  }

  public Integer getSocketTimeout() {
    return socketTimeout;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public boolean isInsecure() {
    return insecure;
  }
}
