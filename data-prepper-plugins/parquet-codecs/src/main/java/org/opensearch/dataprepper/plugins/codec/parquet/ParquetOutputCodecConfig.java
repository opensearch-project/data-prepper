/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class ParquetOutputCodecConfig {

    private static final String DEFAULT_OBJECT_NAME_PATTERN = "events-%{yyyy-MM-dd'T'hh-mm-ss}";
    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("schema_file_location")
    private String fileLocation;

    @JsonProperty("schema_bucket")
    private String schemaBucket;
    @JsonProperty("file_key")
    private String fileKey;
    @JsonProperty("schema_region")
    private String schemaRegion;

    @JsonProperty("region")
    @NotNull
    @Valid
    private String region;

    @JsonProperty("bucket")
    @NotNull
    @Valid
    private String bucket;

    @JsonProperty("path_prefix")
    @NotNull
    @Valid
    private String pathPrefix;
    @JsonProperty("exclude_keys")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("schema_registry_url")
    private String schemaRegistryUrl;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Read s3 object index file pattern configuration.
     *
     * @return default object name pattern.
     */
    public String getNamePattern() {
        return DEFAULT_OBJECT_NAME_PATTERN;
    }
    public void setRegion(String region) {
        this.region = region;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }
    public String getSchemaBucket() {
        return schemaBucket;
    }

    public String getFileKey() {
        return fileKey;
    }

    public String getSchemaRegion() {
        return schemaRegion;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }
    public void setSchemaBucket(String schemaBucket) {
        this.schemaBucket = schemaBucket;
    }

    public void setSchemaRegion(String schemaRegion) {
        this.schemaRegion = schemaRegion;
    }
    public void setFileLocation(String fileLocation) {
        this.fileLocation = fileLocation;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }
}

