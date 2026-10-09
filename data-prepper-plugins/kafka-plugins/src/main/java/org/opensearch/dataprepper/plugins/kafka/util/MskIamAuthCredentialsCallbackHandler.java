/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.util.List;
import java.util.Map;

/**
 * SASL client callback handler for the {@code AWS_MSK_IAM} mechanism that sources credentials
 * exclusively from Data Prepper's own header-aware STS provider, so the configured
 * {@code aws.sts_header_overrides} are applied on the data-plane {@code sts:AssumeRole} used for
 * broker authentication.
 */
class MskIamAuthCredentialsCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MskIamAuthCredentialsCallbackHandler.class);

    @Override
    public void configure(final Map<String, ?> configs,
                          final String saslMechanism,
                          final List<AppConfigurationEntry> jaasConfigEntries) {
        if (!IAMLoginModule.MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        }
    }

    @Override
    public void handle(final Callback[] callbacks) throws UnsupportedCallbackException {
        for (final Callback callback : callbacks) {
            if (callback instanceof AWSCredentialsCallback) {
                handleCallback((AWSCredentialsCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback,
                        "Unsupported callback type: " + callback.getClass().getName());
            }
        }
    }

    private void handleCallback(final AWSCredentialsCallback callback) {
        final AwsCredentialsProvider provider = KafkaSecurityConfigurer.getMskCredentialsProvider();
        try {
            LOG.debug("Resolving MSK IAM credentials from header-aware provider {}",
                    provider == null ? "null" : provider.getClass().getSimpleName());
            callback.setAwsCredentials(toV1Credentials(provider.resolveCredentials()));
        } catch (final Exception e) {
            LOG.debug("Failed to resolve MSK IAM credentials with STS header overrides", e);
            callback.setLoadingException(e);
        }
    }

    @Override
    public void close() {
    }

    private static AWSCredentials toV1Credentials(final AwsCredentials credentials) {
        if (credentials instanceof AwsSessionCredentials) {
            final AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
            return new BasicSessionCredentials(
                    sessionCredentials.accessKeyId(),
                    sessionCredentials.secretAccessKey(),
                    sessionCredentials.sessionToken());
        }
        return new BasicAWSCredentials(credentials.accessKeyId(), credentials.secretAccessKey());
    }
}
