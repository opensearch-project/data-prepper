/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public class ParquetOutputCodecConfig {

    private static final String DEFAULT_OBJECT_NAME_PATTERN = "events-%{yyyy-MM-dd'T'hh-mm-ss}";

    @JsonProperty("schema")
    private String schema;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("schema_file_location")
    private String fileLocation;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("schema_bucket")
    private String schemaBucket;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("file_key")
    private String fileKey;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("schema_region")
    private String schemaRegion;

    @JsonProperty("path_prefix")
    @NotNull
    @Valid
    private String pathPrefix;

    @Valid
    @Size(max = 0, message = "Schema from Schema Registry is not supported.")
    @JsonProperty("schema_registry_url")
    private String schemaRegistryUrl;

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

