package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.apache.kafka.common.security.authenticator.SaslClientCallbackHandler;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class DynamicSaslClientCallbackHandler extends SaslClientCallbackHandler {
    private final DynamicBasicCredentialsProvider dynamicBasicCredentialsProvider = DynamicBasicCredentialsProvider
            .getInstance();

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        super.handle(callbacks);
        final BasicCredentials basicCredentials = dynamicBasicCredentialsProvider.getBasicCredentials();
        for (final Callback callback: callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nc = (NameCallback) callback;
                if (basicCredentials.getUsername() != null) {
                    nc.setName(basicCredentials.getUsername());
                } else {
                    nc.setName(nc.getDefaultName());
                }
            } else if (callback instanceof PasswordCallback) {
                PasswordCallback nc = (PasswordCallback) callback;
                if (basicCredentials.getPassword() == null) {
                    String errorMessage = "Could not login: the client is being asked for a password.";
                    throw new UnsupportedCallbackException(callback, errorMessage);
                }

                final char[] password = basicCredentials.getPassword().toCharArray();
                nc.setPassword(password);
            }
        }
    }
}
