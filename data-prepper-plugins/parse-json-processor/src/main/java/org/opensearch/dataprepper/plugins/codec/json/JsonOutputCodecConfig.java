/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

public class JsonOutputCodecConfig {
    static final String DEFAULT_KEY_NAME = "events";

    @JsonProperty(value = "key_name", defaultValue = DEFAULT_KEY_NAME)
    @Size(min = 1, max = 2048)
    private String keyName = DEFAULT_KEY_NAME;

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }
}
