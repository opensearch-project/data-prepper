/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.schema.validation.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;

public class SchemaValidationConfig {
    @NotNull
    @JsonProperty("schema_type")
    @JsonPropertyDescription("The type of input schema (e.g., 'office365')")
    private String schemaType;

    public String getSchemaType() {
        return schemaType;
    }

}
