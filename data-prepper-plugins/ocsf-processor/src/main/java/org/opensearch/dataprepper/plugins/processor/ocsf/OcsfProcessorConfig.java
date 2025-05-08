/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public class OcsfProcessorConfig {
    @NotNull
    @JsonProperty("schema_mapping")
    @JsonPropertyDescription("The path to the schema mapping file that defines how to transform events to OCSF format.")
    private String schemaMapping;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of strings with which to tag events when the processor fails to transform the event to OCSF format. Defaults to '_ocsf_transform_failure'.")
    private List<String> tagsOnFailure = List.of("_ocsf_transform_failure");

    public String getSchemaMapping() {
        return schemaMapping;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}