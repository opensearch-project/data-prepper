package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;

public class AuthConfig {
    @JsonPropertyDescription("A string that contains the username and is used in the " +
            "<a href=\"https://opensearch.org/docs/latest/security/access-control/users-roles/\">internal users</a> YAML configuration file of your OpenSearch cluster.")
    private String username;

    @JsonPropertyDescription("A string that contains the password and is used in the " +
            "<a href=\"https://opensearch.org/docs/latest/security/access-control/users-roles/\">internal users</a> YAML configuration file of your OpenSearch cluster.")
    private String password;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
