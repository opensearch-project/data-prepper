/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

/**
 * Configuration class for {@link AvroOutputCodec}.
 */
public class AvroOutputCodecConfig {
    @JsonProperty("schema")
    private String schema;

    @JsonProperty("auto_schema")
    private boolean autoSchema;

    @AssertTrue(message = "The Avro codec requires either defining a schema or setting auto_schema to true to automatically generate a schema.")
    boolean isSchemaOrAutoSchemaDefined() {
        return schema != null ^ autoSchema;
    }

    public String getSchema() {
        return schema;
    }

    void setSchema(final String schema) {
        this.schema = schema;
    }

    public boolean isAutoSchema() {
        return autoSchema;
    }

    void setAutoSchema(final boolean autoSchema) {
        this.autoSchema = autoSchema;
    }
}
