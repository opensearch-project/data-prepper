/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;

import java.util.stream.Stream;

/**
 * A helper class that helps to read auth related configuration values from
 * pipelines.yaml
 */
public class AuthConfig {

    public static class SaslAuthConfig {
        @JsonAlias("plain")
        @JsonProperty("plaintext")
        private PlainTextAuthConfig plainTextAuthConfig;

        @JsonProperty("scram")
        private ScramAuthConfig scramAuthConfig;

        @JsonProperty("oauth")
        private OAuthConfig oAuthConfig;

        @JsonProperty("aws_msk_iam")
        private AwsIamAuthConfig awsIamAuthConfig;

        @JsonProperty("ssl_endpoint_identification_algorithm")
        private String sslEndpointAlgorithm;

        public AwsIamAuthConfig getAwsIamAuthConfig() {
            return awsIamAuthConfig;
        }

        public PlainTextAuthConfig getPlainTextAuthConfig() {
            return plainTextAuthConfig;
        }

        public ScramAuthConfig getScramAuthConfig() {
            return scramAuthConfig;
        }

        public OAuthConfig getOAuthConfig() {
            return oAuthConfig;
        }

        public String getSslEndpointAlgorithm() {
            return sslEndpointAlgorithm;
        }

        @AssertTrue(message = "Only one of AwsIam or oAuth or SCRAM or PlainText auth config must be specified")
        public boolean hasOnlyOneConfig() {
            return Stream.of(awsIamAuthConfig, plainTextAuthConfig, oAuthConfig, scramAuthConfig).filter(n -> n != null).count() == 1;
        }

    }


    /*
     * TODO 
        public static class SslAuthConfig {
            // TODO Add Support for SSL authentication types like
            // one-way or two-way authentication

            public SslAuthConfig() {
            }
        }

        @JsonProperty("ssl")
        private SslAuthConfig sslAuthConfig;

        public SslAuthConfig getSslAuthConfig() {
            return sslAuthConfig;
        }

    */

    @Valid
    @JsonProperty("sasl")
    private SaslAuthConfig saslAuthConfig;

    public SaslAuthConfig getSaslAuthConfig() {
        return saslAuthConfig;
    }

    /*
     * Currently SSL config is not supported. Commenting this for future use
     *
    @AssertTrue(message = "Only one of SSL or SASL auth config must be specified")
    public boolean hasSaslOrSslConfig() {
        return Stream.of(sslAuthConfig, saslAuthConfig).filter(n -> n!=null).count() == 1;
    }
    */

}
