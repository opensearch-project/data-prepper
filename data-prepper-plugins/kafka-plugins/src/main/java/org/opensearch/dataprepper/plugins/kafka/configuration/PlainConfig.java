package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class PlainConfig {
    @JsonProperty("username")
    @NotNull
    private String userName;

    @JsonProperty("password")
    @NotNull
    private String password;

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public PlainConfig() {

    }
}
