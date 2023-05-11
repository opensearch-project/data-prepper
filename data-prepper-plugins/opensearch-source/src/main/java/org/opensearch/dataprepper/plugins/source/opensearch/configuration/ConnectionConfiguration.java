/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.List;

public class ConnectionConfiguration {

  @NotNull
  @JsonProperty("hosts")
  private List<String> hosts;

  @JsonProperty("username")
  private String username;

  @JsonProperty("password")
  private String password;

  @JsonProperty("cert")
  private Path certPath;

  @JsonProperty("socket_timeout")
  private Integer socketTimeout;

  @JsonProperty("connection_timeout")
  private Integer connectTimeout;

  @JsonProperty("insecure")
  private boolean insecure;

  public List<String> getHosts() {
    return hosts;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Path getCertPath() {
    return certPath;
  }

  public Integer getSocketTimeout() {
    return socketTimeout;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

}
