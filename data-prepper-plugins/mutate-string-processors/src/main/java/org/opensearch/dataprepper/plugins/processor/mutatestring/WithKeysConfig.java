/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

public abstract class WithKeysConfig implements StringProcessorConfig<EventKey> {

    @NotNull
    @NotEmpty
    @JsonProperty("with_keys")
    @JsonPropertyDescription("A list of keys to modify.")
    private List<EventKey> withKeys;

    @Override
    public List<EventKey> getIterativeConfig() {
        return withKeys;
    }
}
