package org.opensearch.dataprepper.plugins.processor.ocsf;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class OcsfProcessorConfig {
    @NotNull
    @JsonProperty("schema_mapping")
    private String schemaMapping;

    public String getSchemaMapping() {
        return schemaMapping;
    }
}