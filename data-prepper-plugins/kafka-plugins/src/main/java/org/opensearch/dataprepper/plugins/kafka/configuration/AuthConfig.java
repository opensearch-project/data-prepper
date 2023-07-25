/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.Valid;

import java.util.stream.Stream;

/**
 * A helper class that helps to read auth related configuration values from
 * pipelines.yaml
 */
public class AuthConfig {

    public static class SaslAuthConfig {
        @JsonProperty("plaintext")
        private PlainTextAuthConfig plainTextAuthConfig;

        @JsonProperty("oauth")
        private OAuthConfig oAuthConfig;

        @JsonProperty("aws_iam")
        private AwsIamAuthConfig awsIamAuthConfig;

        public AwsIamAuthConfig getAwsIamAuthConfig() {
            return awsIamAuthConfig;
        }

        public PlainTextAuthConfig getPlainTextAuthConfig() {
            return plainTextAuthConfig;
        }

        public OAuthConfig getOAuthConfig() {
            return oAuthConfig;
        }

        @AssertTrue(message = "Only one of AwsIam or oAuth or PlainText auth config must be specified")
        public boolean hasOnlyOneConfig() {
            return Stream.of(awsIamAuthConfig, plainTextAuthConfig, oAuthConfig).filter(n -> n!=null).count() == 1;
        }

    }

    public static  class SslAuthConfig {
        // TODO Add Support for SSL authentication types like
        // one-way or two-way authentication

        public SslAuthConfig() {
        }
    }

    @JsonProperty("ssl")
    private SslAuthConfig sslAuthConfig;

    @Valid
    @JsonProperty("sasl")
    private SaslAuthConfig saslAuthConfig;

    @JsonProperty("insecure")
    private Boolean insecure = false;

    public SslAuthConfig getSslAuthConfig() {
        return sslAuthConfig;
    }

    public SaslAuthConfig getSaslAuthConfig() {
        return saslAuthConfig;
    }

    public Boolean getInsecure() {
        return insecure;
    }

    @AssertTrue(message = "Only one of SSL or SASL auth config must be specified")
    public boolean hasSaslOrSslConfig() {
        return Stream.of(sslAuthConfig, saslAuthConfig).filter(n -> n!=null).count() == 1;
    }

}
