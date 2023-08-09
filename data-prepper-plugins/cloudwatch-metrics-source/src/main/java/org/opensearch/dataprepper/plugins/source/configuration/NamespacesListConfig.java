/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 *  Configuration class to get the Namespace List info
 */
public class NamespacesListConfig {

    @JsonProperty("namespace")
    @NotNull
    NamespaceConfig namespaceConfig;

    /**
     * Get Namespace Configuration
     * @return Namespace Config
     */
    public NamespaceConfig getNamespaceConfig() {
        return namespaceConfig;
    }
}