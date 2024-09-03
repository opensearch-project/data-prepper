/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read sasl SCRAM auth configuration values from
 * pipelines.yaml
 */
public class ScramAuthConfig {

    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @JsonProperty("mechanism")
    private String mechanism;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getMechanism() {
        return mechanism;
    }
}
