/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.http.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpSinkAwsServiceTest {

    private HttpSinkConfiguration httpSinkConfiguration;

    private HttpClientBuilder httpClientBuilder;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private AwsAuthenticationOptions awsAuthenticationOptions;

    private AwsCredentialsProvider awsCredentialsProvider;

    @BeforeEach
    public void setup() throws IOException {
        httpSinkConfiguration = mock(HttpSinkConfiguration.class);
        httpClientBuilder = mock(HttpClientBuilder.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::1234567890:role/app-test");
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn("test");
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("ap-south-1"));
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(new HashMap<>());
        when(httpSinkConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsCredentialsSupplier.getProvider(Mockito.any())).thenReturn(awsCredentialsProvider);

    }

    @Test
    public void attachSigV4Test() {
        HttpSinkAwsService.attachSigV4(httpSinkConfiguration,httpClientBuilder,awsCredentialsSupplier);
    }
}
