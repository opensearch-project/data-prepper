package org.opensearch.dataprepper.plugins.kafkaconnect.util;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.UUID;

public class SecretManagerHelper {
    private static final String SESSION_PREFIX = "data-prepper-secretmanager-session";
    public static String getSecretValue(final String stsRoleArn, final String region, final String secretId) {
        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration
                .builder()
                .retryPolicy(RetryPolicy.defaultRetryPolicy())
                .build();

        if (stsRoleArn != null && !stsRoleArn.isEmpty()) {
            String sessionName = SESSION_PREFIX + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .overrideConfiguration(clientOverrideConfiguration)
                    .region(Region.of(region))
                    .credentialsProvider(credentialsProvider)
                    .build();
            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest
                    .builder()
                    .roleArn(stsRoleArn)
                    .roleSessionName(sessionName)
                    .build();
            credentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequest)
                    .build();
        }
        SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder()
                .overrideConfiguration(clientOverrideConfiguration)
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();
        final GetSecretValueRequest request = GetSecretValueRequest.builder().secretId(secretId).build();
        final GetSecretValueResponse response = secretsManagerClient.getSecretValue(request);
        return response.secretString();
    }
}
