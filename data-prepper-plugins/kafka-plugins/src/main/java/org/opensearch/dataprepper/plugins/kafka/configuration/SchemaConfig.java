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

  private static final int SESSION_TIME_OUT = 45000;

  @JsonProperty("registry_url")
  private String registryURL;

  @JsonProperty("version")
  private int version;

  @JsonProperty("schema_registry_api_key")
  private String schemaRegistryApiKey;

  @JsonProperty("schema_registry_api_secret")
  private String schemaRegistryApiSecret;

  @JsonProperty("session_timeout_ms")
  private int sessionTimeoutms = SESSION_TIME_OUT;

  @JsonProperty("basic_auth_credentials_source")
  private String basicAuthCredentialsSource;

  public int getSessionTimeoutms() {
    return sessionTimeoutms;
  }

  public String getBasicAuthCredentialsSource() {
    return basicAuthCredentialsSource;
  }

  public String getRegistryURL() {
    return registryURL;
  }

  public int getVersion() {
    return version;
  }

  public String getSchemaRegistryApiKey() {
    return schemaRegistryApiKey;
  }

  public String getSchemaRegistryApiSecret() {
    return schemaRegistryApiSecret;
  }
}
