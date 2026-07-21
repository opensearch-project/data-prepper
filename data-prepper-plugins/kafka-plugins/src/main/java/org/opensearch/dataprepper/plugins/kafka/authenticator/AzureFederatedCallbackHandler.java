/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AzureFederatedCallbackHandler implements AuthenticateCallbackHandler {
    public static final String OPT_REGION = "azureFederatedRegion";
    public static final String OPT_STS_ROLE_ARN = "azureFederatedStsRoleArn";
    public static final String OPT_TOKEN_ENDPOINT = "azureFederatedTokenEndpoint";
    public static final String OPT_CLIENT_ID = "azureFederatedClientId";
    public static final String OPT_SCOPE = "azureFederatedScope";
    public static final String OPT_STS_HEADER_OVERRIDES = "azureFederatedStsHeaderOverrides";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private AzureFederatedTokenProvider tokenProvider;

    public AzureFederatedCallbackHandler() {
    }

    AzureFederatedCallbackHandler(final AzureFederatedTokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(final Map<String, ?> configs, final String saslMechanism,
                          final List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        }
        final Map<String, String> options = (Map<String, String>) jaasConfigEntries.get(0).getOptions();
        final Map<String, String> stsHeaderOverrides = decodeStsHeaderOverrides(options.get(OPT_STS_HEADER_OVERRIDES));
        final AwsCredentialsSupplier awsCredentialsSupplier =
                AwsCredentialsSupplierProvider.getInstance().getAwsCredentialsSupplier();
        this.tokenProvider = new AzureFederatedTokenProvider(
                options.get(OPT_REGION), options.get(OPT_STS_ROLE_ARN), options.get(OPT_TOKEN_ENDPOINT),
                options.get(OPT_CLIENT_ID), options.get(OPT_SCOPE), stsHeaderOverrides, awsCredentialsSupplier);
    }

    private static Map<String, String> decodeStsHeaderOverrides(final String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            final byte[] json = Base64.getDecoder().decode(encoded);
            return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (final IOException e) {
            throw new RuntimeException("Failed to decode azure_federated STS header overrides", e);
        }
    }

    @Override
    public void handle(final Callback[] callbacks) throws UnsupportedCallbackException {
        for (final Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                ((OAuthBearerTokenCallback) callback).token(tokenProvider.getToken());
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() {
        if (tokenProvider != null) {
            tokenProvider.close();
        }
    }

    AzureFederatedTokenProvider getTokenProvider() {
        return tokenProvider;
    }
}
