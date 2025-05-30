/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class EncryptionPluginConfig {

    @JsonAnySetter
    private Map<String, EncryptionEngineConfiguration> encryptionConfigurationMap = new HashMap<>();

    public Map<String, EncryptionEngineConfiguration> getEncryptionConfigurationMap() {
        return encryptionConfigurationMap;
    }
}
