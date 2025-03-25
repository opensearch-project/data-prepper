package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthConfig {
    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @JsonProperty("apitoken")
    private String apitoken;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getApitoken() {
        return apitoken;
    }
}
