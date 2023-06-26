/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public class KeysConfig {

    @JsonProperty("key")
    @NotNull
    @Valid
    private KeyConfig keyConfig;

    /**
     * Get the Configured source target and attributes options
     * @return KeyConfig
     */
    public KeyConfig getKeyConfig() {
        return keyConfig;
    }
}
