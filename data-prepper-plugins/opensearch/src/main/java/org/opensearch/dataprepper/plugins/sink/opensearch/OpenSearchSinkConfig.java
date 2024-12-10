package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linecorp.armeria.server.annotation.Get;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.opensearch.AuthConfig;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;

import java.util.List;
import java.util.Objects;

public class OpenSearchSinkConfig {
    @Getter
    @JsonProperty("hosts")
    private List<String> hosts;

    @Getter
    @JsonProperty("username")
    private String username = null;

    @Getter
    @JsonProperty("password")
    private String password = null;

    @Getter
    @JsonProperty("authconfig")
    private AuthConfig authConfig = null;

    @Getter
    @JsonProperty("socket_timeout")
    private Integer socketTimeout = null;

    @Getter
    @JsonProperty("connect_timeout")
    private Integer connectTimeout = null;

    @Getter
    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationConfiguration awsAuthenticationOptions;

    @Getter
    @Deprecated
    @JsonProperty("aws_sigv4")
    private Boolean awsSigv4 = false;

    @Getter
    @JsonProperty("cert")
    private String certPath = null;

    @Getter
    @JsonProperty("insecure")
    private Boolean insecure = false;

    @Getter
    @JsonProperty("proxy")
    private String proxy = null;

    @Getter
    @JsonProperty("distribution_version")
    private String distributionVersion = DistributionVersion.DEFAULT.getVersion();

    @JsonProperty("enable_request_compression")
    private Boolean enableRequestCompression = null;

    public Boolean getEnableRequestCompression(boolean defaultValue) {
        return Objects.requireNonNullElse(enableRequestCompression, defaultValue);
    }

    public void validateConfig() {
        isValidAuthConfig();
    }


    void isValidAuthConfig() {
        if (authConfig != null) {
            if (username != null || password != null) {
                throw new IllegalStateException("Deprecated username and password should not be set " +
                        "when authentication is configured.");
            }
        }
    }
}

