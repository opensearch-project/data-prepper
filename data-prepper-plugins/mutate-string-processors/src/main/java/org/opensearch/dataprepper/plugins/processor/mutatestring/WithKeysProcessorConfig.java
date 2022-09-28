/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public abstract class WithKeysProcessorConfig implements StringProcessorConfig<String> {
    @NotEmpty
    @NotNull
    @JsonProperty("with_keys")
    private List<String> withKeys;

    @Override
    public List<String> getIterativeConfig() {
        return withKeys;
    }

    public List<String> getWithKeys() {
        return withKeys;
    }
}
