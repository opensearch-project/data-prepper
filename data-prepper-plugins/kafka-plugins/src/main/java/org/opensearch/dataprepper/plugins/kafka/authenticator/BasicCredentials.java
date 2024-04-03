package org.opensearch.dataprepper.plugins.kafka.authenticator;

public class BasicCredentials {
    private final String username;
    private final String password;

    public BasicCredentials(final String username, final String password) {
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
