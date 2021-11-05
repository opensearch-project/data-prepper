package com.amazon.dataprepper.armeria.authentication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for HTTP Basic Authentication.
 *
 * @since 1.2
 */
public class HttpBasicAuthenticationConfig {
    private final String username;
    private final String password;

    @JsonCreator
    public HttpBasicAuthenticationConfig(
            @JsonProperty("username") final String username,
            @JsonProperty("password") final String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
