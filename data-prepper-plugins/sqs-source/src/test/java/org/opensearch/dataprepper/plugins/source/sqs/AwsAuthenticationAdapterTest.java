/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
class AwsAuthenticationAdapterTest {
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private SqsSourceConfig sqsSourceConfig;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private String stsRoleArn;

    @BeforeEach
    void setUp() {
        when(sqsSourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

        stsRoleArn = UUID.randomUUID().toString();
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
    }

    private AwsAuthenticationAdapter createObjectUnderTest() {
        return new AwsAuthenticationAdapter(awsCredentialsSupplier, sqsSourceConfig);
    }

    @Test
    void getCredentialsProvider_returns_AwsCredentialsProvider_from_AwsCredentialsSupplier() {
        final AwsCredentialsProvider expectedProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(expectedProvider);

        assertThat(createObjectUnderTest().getCredentialsProvider(), equalTo(expectedProvider));
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "eu-west-1"})
    void getCredentialsProvider_creates_expected_AwsCredentialsOptions(final String regionString) {
        final String externalId = UUID.randomUUID().toString();
        final Region region = Region.of(regionString);

        final Map<String, String> headerOverrides = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(externalId);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(headerOverrides);

        createObjectUnderTest().getCredentialsProvider();

        final ArgumentCaptor<AwsCredentialsOptions> credentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(credentialsOptionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualOptions = credentialsOptionsArgumentCaptor.getValue();

        assertThat(actualOptions, notNullValue());
        assertThat(actualOptions.getStsRoleArn(), equalTo(stsRoleArn));
        assertThat(actualOptions.getStsExternalId(), equalTo(externalId));
        assertThat(actualOptions.getRegion(), equalTo(region));
        assertThat(actualOptions.getStsHeaderOverrides(), equalTo(headerOverrides));
    }
}