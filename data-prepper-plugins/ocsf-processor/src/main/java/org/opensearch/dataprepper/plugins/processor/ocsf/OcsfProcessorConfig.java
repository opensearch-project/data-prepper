/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class OcsfProcessorConfig {
    public static final String DEFAULT_VERSION = "1.1";

    @JsonProperty("schema_type")
    @JsonPropertyDescription("schema type of the ocsf processor")
    @UsesDataPrepperPlugin(pluginType = OcsfTransformer.class)
    @NotNull
    PluginModel schemaType = null;

    public PluginModel getSchemaType() {
        return schemaType;
    }

    @JsonProperty("version")
    @JsonPropertyDescription("Target OCSF version")
    String version = DEFAULT_VERSION;

    public String getVersion() {
         return version;
    }
}
