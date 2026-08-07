/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;

import java.util.Map;

public class ScriptConfiguration {

    @Getter
    @NotEmpty
    @JsonProperty("source")
    private String source;

    @Getter
    @JsonProperty("params")
    private Map<String, Object> params;
}
