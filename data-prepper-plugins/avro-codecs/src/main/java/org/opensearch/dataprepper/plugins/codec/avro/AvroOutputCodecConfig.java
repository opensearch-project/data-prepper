/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;


public class AvroOutputCodecConfig {

    @JsonProperty("schema")
    private final String schema;

    public AvroOutputCodecConfig(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }
}
