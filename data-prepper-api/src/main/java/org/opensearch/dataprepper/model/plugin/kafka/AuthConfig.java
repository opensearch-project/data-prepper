/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A helper class that helps to read auth related configuration values from
 * pipelines.yaml
 */
public class AuthConfig {

    @JsonProperty("sasl")
    private SaslAuthConfig saslAuthConfig;

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

    public SaslAuthConfig getSaslAuthConfig() {
        return saslAuthConfig;
    }

    public static class SaslAuthConfig {
        @JsonProperty("plaintext")
        private PlainTextAuthConfig plainTextAuthConfig;

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

        public OAuthConfig getOAuthConfig() {
            return oAuthConfig;
        }

        public String getSslEndpointAlgorithm() {
            return sslEndpointAlgorithm;
        }
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
