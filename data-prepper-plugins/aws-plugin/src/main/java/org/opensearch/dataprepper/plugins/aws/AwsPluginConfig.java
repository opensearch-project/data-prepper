/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.aws.common.AwsStsConfiguration;

public class AwsPluginConfig {

    @JsonProperty("default")
    private AwsStsConfiguration defaultStsConfiguration = new AwsStsConfiguration();

    public AwsStsConfiguration getDefaultStsConfiguration() {
        return defaultStsConfiguration;
    }
}
