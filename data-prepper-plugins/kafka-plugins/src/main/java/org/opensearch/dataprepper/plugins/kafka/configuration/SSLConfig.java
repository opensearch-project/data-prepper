package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SSLConfig {

    @JsonProperty("ssl_truststore_location")
    private String trustStoreLocation;

    @JsonProperty("ssl_truststore_password")
    private String trustStorePassword;

    @JsonProperty("ssl_keystore_location")
    private String keyStoreLocation;

    @JsonProperty("ssl_keystore_password")
    private String keyStorePassword;

    @JsonProperty("ssl_key_password")
    private String keyPassword;

    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }
}
