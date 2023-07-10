/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.stream.Stream;

/**
 * A helper class that helps to read auth related configuration values from
 * pipelines.yaml
 */
public class AuthConfig {

    public class MskIamConfig {
    }

    public class SaslAuthConfig {
        @JsonProperty("plaintext")
        private PlainTextAuthConfig plainTextAuthConfig;

        @JsonProperty("oauth")
        private OAuthConfig oAuthConfig;

        @JsonProperty("msk_iam")
        private MskIamConfig mskIamConfig;

        public MskIamConfig getMskIamConfig() {
            return mskIamConfig;
        }

        public PlainTextAuthConfig getPlainTextAuthConfig() {
            return plainTextAuthConfig;
        }

        public OAuthConfig getOAuthConfig() {
            return oAuthConfig;
        }
    }

    public class SslAuthConfig {
        // TODO Add Support for SSL authentication types like
        // one-way or two-way authentication
    }

    @JsonProperty("ssl")
    private SslAuthConfig sslAuthConfig;

    @JsonProperty("sasl")
    private SaslAuthConfig saslAuthConfig;

    public SslAuthConfig getSslAuthConfig() {
        return sslAuthConfig;
    }

    public SaslAuthConfig getSaslAuthConfig() {
        return saslAuthConfig;
    }

    @AssertTrue(message = "Only one of SSL or SASL auth config must be specified")
    public boolean hasSaslOrSslConfig() {
        return Stream.of(sslAuthConfig, saslAuthConfig).filter(n -> n!=null).count() == 1;
    }

}
