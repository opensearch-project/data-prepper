/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

class CredentialsProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CredentialsProviderFactory.class);
    private static final String AWS_IAM = "iam";
    private static final String AWS_IAM_ROLE = "role";

    AwsCredentialsProvider providerFromOptions(final AwsCredentialsOptions credentialsOptions) {
        Objects.requireNonNull(credentialsOptions);

        if(credentialsOptions.getStsRoleArn() != null) {
            return createStsCredentials(credentialsOptions);
        }

        return DefaultCredentialsProvider.create();
    }

    private AwsCredentialsProvider createStsCredentials(final AwsCredentialsOptions credentialsOptions) {

        final String stsRoleArn = credentialsOptions.getStsRoleArn();

        validateStsRoleArn(stsRoleArn);

        LOG.debug("Creating new AwsCredentialsProvider with role {}.", stsRoleArn);

        StsClientBuilder stsClientBuilder = StsClient.builder();

        stsClientBuilder = Optional.ofNullable(credentialsOptions.getRegion())
                .map(stsClientBuilder::region)
                .orElse(stsClientBuilder);

        final StsClient stsClient = stsClientBuilder.build();

        AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                .roleSessionName("Data-Prepper-" + UUID.randomUUID())
                .roleArn(stsRoleArn);

        final Map<String, String> awsStsHeaderOverrides = credentialsOptions.getStsHeaderOverrides();

        if(awsStsHeaderOverrides != null && !awsStsHeaderOverrides.isEmpty()) {
            assumeRoleRequestBuilder = assumeRoleRequestBuilder
                    .overrideConfiguration(configuration -> awsStsHeaderOverrides.forEach(configuration::putHeader));
        }

        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(assumeRoleRequestBuilder.build())
                .build();
    }

    private void validateStsRoleArn(final String stsRoleArn) {
        final Arn arn = getArn(stsRoleArn);
        if (!AWS_IAM.equals(arn.service())) {
            throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
        }
    }

    private Arn getArn(final String stsRoleArn) {
        try {
            return Arn.fromString(stsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", stsRoleArn));
        }
    }
}
