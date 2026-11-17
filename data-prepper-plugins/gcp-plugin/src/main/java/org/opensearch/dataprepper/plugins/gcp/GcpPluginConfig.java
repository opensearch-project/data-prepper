/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import org.opensearch.dataprepper.gcp.api.GcpConfig;

/**
 * The {@code extensions.gcp.configurations} block. Only {@code default} is supported in this version.
 */
public class GcpPluginConfig {
    @JsonProperty("default")
    @Valid
    private GcpConfig defaultConfig;

    public GcpConfig getDefaultConfig() {
        return defaultConfig;
    }
}
