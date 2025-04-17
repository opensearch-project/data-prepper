package org.opensearch.dataprepper.plugins.source.crowdstrike.configuration;

import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for authentication with the CrowdStrike API.
 */
@Getter
public class AuthenticationConfig {

    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("client_secret")
    private String clientSecret;

    @AssertTrue(message = "Client Id and Client Secret are both required for Authentication")
    public boolean isValidConfig() {
        return clientId != null && clientSecret != null;
    }

}
