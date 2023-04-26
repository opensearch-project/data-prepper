/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;

/**
 * * A helper class that helps to read ssl authentication config values from
 * pipelines.yaml
 */

public class SSLAuthConfig {

  @JsonProperty("ssl_truststore_location")
  @Valid
  private String sslTruststoreLocation;

  @JsonProperty("ssl_truststore_password")
  @Valid
  private String sslTruststorePassword;

  @JsonProperty("ssl_keystore_location")
  @Valid
  private String sslKeystoreLocation;

  @JsonProperty("ssl_keystore_password")
  @Valid
  private String sslKeystorePassword;

  @JsonProperty("ssl_key_password")
  @Valid
  private String sslKeyPassword;

  public String getSslTruststoreLocation() {
	return sslTruststoreLocation;
  }

  public void setSslTruststoreLocation(String sslTruststoreLocation) {
	this.sslTruststoreLocation = sslTruststoreLocation;
  }

  public String getSslTruststorePassword() {
	return sslTruststorePassword;
  }

  public void setSslTruststorePassword(String sslTruststorePassword) {
	this.sslTruststorePassword = sslTruststorePassword;
  }

  public String getSslKeystoreLocation() {
	return sslKeystoreLocation;
  }

  public void setSslKeystoreLocation(String sslKeystoreLocation) {
	this.sslKeystoreLocation = sslKeystoreLocation;
  }

  public String getSslKeystorePassword() {
	return sslKeystorePassword;
  }

  public void setSslKeystorePassword(String sslKeystorePassword) {
	this.sslKeystorePassword = sslKeystorePassword;
  }

  public String getSslKeyPassword() {
	return sslKeyPassword;
  }

  public void setSslKeyPassword(String sslKeyPassword) {
	this.sslKeyPassword = sslKeyPassword;
  }

}
