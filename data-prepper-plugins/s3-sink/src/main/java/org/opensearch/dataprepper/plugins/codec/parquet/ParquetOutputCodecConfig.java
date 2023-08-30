/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

public class ParquetOutputCodecConfig {
    @JsonProperty("schema")
    private String schema;

    @JsonProperty("auto_schema")
    private boolean autoSchema;

    @AssertTrue(message = "The Parquet codec requires either defining a schema or setting auto_schema to true to automatically generate a schema.")
    boolean isSchemaOrAutoSchemaDefined() {
        return schema != null ^ autoSchema;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(final String schema) {
        this.schema = schema;
    }

    public boolean isAutoSchema() {
        return autoSchema;
    }

    void setAutoSchema(final boolean autoSchema) {
        this.autoSchema = autoSchema;
    }
}

