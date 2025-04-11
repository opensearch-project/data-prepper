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
import com.fasterxml.jackson.core.StreamReadConstraints;
import jakarta.validation.constraints.Min;
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

    @JsonProperty("max_event_length")
    @Min(StreamReadConstraints.DEFAULT_MAX_STRING_LEN)
    private Integer maxEventLength;

    public Integer getMaxEventLength(){
        return maxEventLength;
    }
}
