/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.stream.Stream;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class SchemaConfig {

  private static final int SESSION_TIME_OUT = 45000;

  @JsonProperty("type")
  private SchemaRegistryType type;

  @JsonProperty("registry_url")
  private String registryURL;

  @JsonProperty("version")
  private int version;

  @JsonAlias("schema_registry_api_key")
  @JsonProperty("api_key")
  private String schemaRegistryApiKey;

  @JsonAlias("schema_registry_api_secret")
  @JsonProperty("api_secret")
  private String schemaRegistryApiSecret;

  @JsonProperty("session_timeout_ms")
  private int sessionTimeoutms = SESSION_TIME_OUT;

  @JsonProperty("basic_auth_credentials_source")
  private String basicAuthCredentialsSource;

  @JsonProperty("inline_schema")
  private String inlineSchema;

  @JsonProperty("schema_file_location")
  private String schemaFileLocation;

  @JsonProperty("s3_file_config")
  private S3FileConfig s3FileConfig;

  @JsonProperty("is_schema_create")
  @NotNull
  private Boolean isSchemaCreate=Boolean.FALSE;

  public static class S3FileConfig {
    @Valid
    @Size(max = 0, message = "bucket is mandatory.")
    @JsonProperty("bucket_name")
    private String bucketName;

    @Valid
    @Size(max = 0, message = "file key is mandatory.")
    @JsonProperty("file_key")
    private String fileKey;

    @Valid
    @Size(max = 0, message = "region is mandatory")
    @JsonProperty("region")
    private String region;

    public String getRegion() {
      return region;
    }

    public S3FileConfig() {
    }

    public String getBucketName() {
      return bucketName;
    }

    public String getFileKey() {
      return fileKey;
    }
  }

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

  public SchemaRegistryType getType() {
    return type;
  }

  public String getSchemaRegistryApiKey() {
    return schemaRegistryApiKey;
  }

  public String getSchemaRegistryApiSecret() {
    return schemaRegistryApiSecret;
  }


  public String getInlineSchema() {
    return inlineSchema;
  }

  public String getSchemaFileLocation() {
    return schemaFileLocation;
  }

  public S3FileConfig getS3FileConfig() {
    return s3FileConfig;
  }

  @AssertTrue(message = "Only one of Inline schema or Schema file location or S3 file config  must be specified")
  public boolean hasOnlyOneConfig() {
    if(isSchemaCreate) {
      return Stream.of(inlineSchema, schemaFileLocation, s3FileConfig).filter(n -> n != null).count() == 1;
    }
    return true;
  }

  public Boolean isCreate() {
    return isSchemaCreate;
  }
}
