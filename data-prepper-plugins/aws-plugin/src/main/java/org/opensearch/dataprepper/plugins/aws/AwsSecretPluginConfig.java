/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AwsSecretPluginConfig {
    @JsonAnySetter
    private Map<String, AwsSecretManagerConfiguration> awsSecretManagerConfigurationMap = new HashMap<>();

    public Map<String, AwsSecretManagerConfiguration> getAwsSecretManagerConfigurationMap() {
        return Collections.unmodifiableMap(awsSecretManagerConfigurationMap);
    }
}
