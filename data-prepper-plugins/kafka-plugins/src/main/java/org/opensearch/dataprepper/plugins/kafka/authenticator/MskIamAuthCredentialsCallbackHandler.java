/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.authenticator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

/**
 * MSK IAM SASL callback handler that supplies credentials from an STS AssumeRole call that applies
 * the configured {@code sts_header_overrides}. The stock {@link IAMClientCallbackHandler} performs
 * the role assumption without applying any STS header overrides. This reuses everything from the
 * stock handler except the source of credentials.
 *
 * <p>The header-aware provider is AWS SDK v2 while aws-msk-iam-auth 2.0.3 expects SDK v1
 * {@link AWSCredentials}, so the resolved temporary credentials are translated v2 to v1.
 */
public class MskIamAuthCredentialsCallbackHandler extends IAMClientCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MskIamAuthCredentialsCallbackHandler.class);

    @Override
    protected void handleCallback(final AWSCredentialsCallback callback) {
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
