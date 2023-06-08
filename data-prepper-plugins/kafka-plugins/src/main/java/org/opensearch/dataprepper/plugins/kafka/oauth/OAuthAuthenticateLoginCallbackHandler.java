/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.oauth;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OAuthAuthenticateLoginCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthAuthenticateLoginCallbackHandler.class);
    private Map<String, String> moduleOptions = null;
    private boolean configured = false;

    @Override
    public void configure(Map<String, ?> map, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Invalid SASL mechanism: %s", saslMechanism));
        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null)
            throw new IllegalArgumentException(
                    String.format("Must supply exactly one non-null JAAS mechanism configuration (size is : %d)",
                            jaasConfigEntries.size()));
        this.moduleOptions = Collections.unmodifiableMap((Map<String, String>) jaasConfigEntries.get(0).getOptions());

        configured = true;
    }

    public boolean isConfigured() {
        return this.configured;
    }

    @Override
    public void close() {
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (!isConfigured())
            throw new IllegalStateException("Callback handler not configured properly...");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback)
                try {
                    handleCallback((OAuthBearerTokenCallback) callback);
                } catch (KafkaException e) {
                    throw new IOException(e.getMessage(), e);
                }
            else
                throw new UnsupportedCallbackException(callback);
        }
    }

    private void handleCallback(OAuthBearerTokenCallback callback) {
        if (callback.token() != null)
            throw new IllegalArgumentException("Callback already received a token");

        LOG.info("Trying to acquire token...!");
        OAuthBearerTokenJwt token = OAuthHttpCalls.login(this.moduleOptions);
        LOG.info("Retrieved the Token..");
        if (token == null) {
            throw new IllegalArgumentException("Null or empty token returned from the server...");
        }
        callback.token(token);
    }

}
