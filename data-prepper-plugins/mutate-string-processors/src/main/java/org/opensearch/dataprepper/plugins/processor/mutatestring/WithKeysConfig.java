/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

@JsonClassDescription("https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/trim-string/")
public class WithKeysConfig implements StringProcessorConfig<EventKey> {

    @NotNull
    @NotEmpty
    @JsonProperty("with_keys")
    @JsonPropertyDescription("A list of keys to trim the white space from.")
    private List<EventKey> withKeys;

    @Override
    public List<EventKey> getIterativeConfig() {
        return withKeys;
    }

    public List<EventKey> getWithKeys() {
        return withKeys;
    }
}
