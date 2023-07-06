/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Configuration class for {@link AvroOutputCodec}.
 */
public class AvroOutputCodecConfig {

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("schema_file_location")
    private String fileLocation;

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys;
    @JsonProperty("schema_registry_url")
    private String schemaRegistryUrl;

    @JsonProperty("region")
    private String region;
    @JsonProperty("bucket_name")
    private String bucketName;
    @JsonProperty("fileKey")
    private String file_key;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getRegion() {
        return region;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getFile_key() {
        return file_key;
    }
    public void setExcludeKeys(List<String> excludeKeys) {
        this.excludeKeys = excludeKeys;
    }

}
