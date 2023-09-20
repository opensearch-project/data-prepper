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
