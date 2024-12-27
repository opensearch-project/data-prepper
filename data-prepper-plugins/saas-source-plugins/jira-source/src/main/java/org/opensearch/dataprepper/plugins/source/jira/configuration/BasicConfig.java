package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;

@Getter
public class BasicConfig {
    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @AssertTrue(message = "Username and Password are both required for Basic Auth")
    private boolean isBasicConfigValid() {
        return username != null && password != null;
    }
}
