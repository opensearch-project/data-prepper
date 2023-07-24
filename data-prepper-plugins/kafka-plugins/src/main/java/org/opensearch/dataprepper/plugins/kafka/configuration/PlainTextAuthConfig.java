/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read sasl plaintext auth configuration values from
 * pipelines.yaml
 */
public class PlainTextAuthConfig {

    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @JsonProperty("security_protocol")
    private String securityProtocol;

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
