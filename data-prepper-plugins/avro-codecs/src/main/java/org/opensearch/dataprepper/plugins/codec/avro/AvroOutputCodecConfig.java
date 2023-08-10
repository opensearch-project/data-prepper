/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;

/**
 * Configuration class for {@link AvroOutputCodec}.
 */
public class AvroOutputCodecConfig {

    @JsonProperty("schema")
    private String schema;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("schema_file_location")
    private String fileLocation;

    @Valid
    @Size(max = 0, message = "Schema from schema registry is not supported.")
    @JsonProperty("schema_registry_url")
    private String schemaRegistryUrl;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("region")
    private String region;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("bucket_name")
    private String bucketName;

    @Valid
    @Size(max = 0, message = "Schema from file is not supported.")
    @JsonProperty("file_key")
    private String fileKey;

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

    public String getFileKey() {
        return fileKey;
    }


}
