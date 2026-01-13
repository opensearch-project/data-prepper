/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AwsPluginConfig {

    @JsonProperty("default")
    private AwsStsConfiguration defaultStsConfiguration = new AwsStsConfiguration();

    public AwsStsConfiguration getDefaultStsConfiguration() {
        return defaultStsConfiguration;
    }
}
