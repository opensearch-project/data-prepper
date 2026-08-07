/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PipelineExtensions {

    @JsonAnySetter
    private final Map<String, Object> extensionMap;

    @JsonCreator
    public PipelineExtensions() {
        extensionMap = new HashMap<>();
    }

    @JsonAnyGetter
    public Map<String, Object> getExtensionMap() {
        return Collections.unmodifiableMap(extensionMap);
    }
}
