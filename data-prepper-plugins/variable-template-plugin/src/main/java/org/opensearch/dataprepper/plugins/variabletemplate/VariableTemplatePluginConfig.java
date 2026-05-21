/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VariableTemplatePluginConfig {

    @JsonProperty("resolvers")
    private Resolvers resolvers = new Resolvers();

    public Resolvers getResolvers() {
        return resolvers;
    }

    public static class Resolvers {

        @JsonProperty("env")
        private boolean envEnabled = false;

        @JsonProperty("file")
        private boolean fileEnabled = false;

        @JsonProperty("store")
        private StoreResolverConfig store = null;

        public boolean isEnvEnabled() {
            return envEnabled;
        }

        public boolean isFileEnabled() {
            return fileEnabled;
        }

        public StoreResolverConfig getStore() {
            return store;
        }
    }

    public static class StoreResolverConfig {

        @JsonProperty("enabled")
        private boolean enabled = false;

        @JsonProperty("sources")
        private java.util.List<String> sources = java.util.Collections.emptyList();

        public boolean isEnabled() {
            return enabled;
        }

        public java.util.List<String> getSources() {
            return sources;
        }
    }
}
