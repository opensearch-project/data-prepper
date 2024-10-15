/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

import java.util.List;

public class JsonInputCodecConfig {

    @JsonProperty("key_name")
    @Size(min = 1, max = 2048)
    private String keyName;

    public String getKeyName() {
        return keyName;
    }

    @JsonProperty("include_keys")
    private List<String> includeKeys;

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    @JsonProperty("include_keys_metadata")
    private List<String> includeKeysMetadata;

    public List<String> getIncludeKeysMetadata() {
        return includeKeysMetadata;
    }
}
