/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AwsPluginConfig {

    @JsonProperty("default")
    private AwsStsConfiguration defaultStsConfiguration = new AwsStsConfiguration();

    public AwsStsConfiguration getDefaultStsConfiguration() {
        return defaultStsConfiguration;
    }

    private Map<String, AwsStsConfiguration> allOtherConfigurations = new HashMap<>();

    @JsonAnyGetter
    public Map<String, AwsStsConfiguration> getAllOtherConfigurations() {
        return allOtherConfigurations;
    }

    @JsonAnySetter
    public void setAdditionalConfiguration(String name, AwsStsConfiguration configuration) {
        allOtherConfigurations.put(name, configuration);
    }

    public Collection<String> listNonDefaultConfigurations() {
        return allOtherConfigurations.keySet();
    }

    public AwsStsConfiguration getConfiguration(String name) {
        return allOtherConfigurations.get(name);
    }
}
