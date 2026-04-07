package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

public class AuthConfig {
    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @JsonProperty("api_token")
    private String apitoken;

    @JsonProperty("client_certificate")
    private String clientCertificate;

    @JsonProperty("client_key")
    private String clientKey;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getApitoken() {
        return apitoken;
    }

    public String getClientCertificate() {
        return clientCertificate;
    }

    public String getClientKey() {
        return clientKey;
    }

    @AssertTrue(message = "client_certificate and client_key must both be provided when using client certificate authentication.")
    public boolean isClientCertificateValid() {
        if (clientCertificate != null || clientKey != null) {
            return clientCertificate != null && clientKey != null;
        }
        return true;
    }
}
