/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("This processor is a [mutate string]" +
        "(https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/mutate-string-processors#mutate-string-processors) processor.")
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
