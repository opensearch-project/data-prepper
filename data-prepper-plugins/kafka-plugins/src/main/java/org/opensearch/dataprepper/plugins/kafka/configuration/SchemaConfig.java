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

  @JsonProperty("key_deserializer")
  private String keyDeserializer;

  @JsonProperty("value_deserializer")
  private String valueDeserializer;

  @JsonProperty("schema_type")
  private String schemaType;

  @JsonProperty("record_type")
  private String recordType;

  public String getRegistryURL() {
    return registryURL;
  }

  public void setRegistryURL(String registryURL) {
    this.registryURL = registryURL;
  }

  public String getKeyDeserializer() {
    return keyDeserializer;
  }

  public void setKeyDeserializer(String keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
  }

  public String getValueDeserializer() {
    return valueDeserializer;
  }

  public void setValueDeserializer(String valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
  }

  public String getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  public String getRecordType() {
    return recordType;
  }

  public void setRecordType(String recordType) {
    this.recordType = recordType;
  }
}
