/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.aws.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsContextImplTest {
    private static final String TEST_STS_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
    @Mock
    private AwsCredentialsConfig awsCredentialsConfig;
    @Mock
    private AwsCredentialsOptions awsCredentialsOptions;

    private AwsContextImpl createObjectUnderTest(final AwsCredentialsConfig awsCredentialsConfig) {
        return new AwsContextImpl(awsCredentialsConfig);
    }

    @Test
    void getOrDefault_uses_defaultOptions_when_awsCredentialsConfig_is_null() {
        final AwsCredentialsProvider awsCredentialsProvider = createObjectUnderTest(null)
                .getOrDefault();
        assertThat(awsCredentialsProvider, instanceOf(DefaultCredentialsProvider.class));
    }

    @Test
    void getOrDefault_uses_uses_awsCredentialsConfig_to_without_sts() {
        when(awsCredentialsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);
        final AwsCredentialsProvider awsCredentialsProvider = createObjectUnderTest(awsCredentialsConfig)
                .getOrDefault();
        assertThat(awsCredentialsProvider, instanceOf(DefaultCredentialsProvider.class));
    }

    @Test
    void getOrDefault_uses_uses_awsCredentialsConfig_to_with_sts() {
        when(awsCredentialsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);
        when(awsCredentialsConfig.getStsRoleArn()).thenReturn(TEST_STS_ROLE_ARN);
        when(awsCredentialsOptions.getStsRoleArn()).thenReturn(TEST_STS_ROLE_ARN);
        when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
        final AwsCredentialsProvider awsCredentialsProvider = createObjectUnderTest(awsCredentialsConfig)
                .getOrDefault();
        assertThat(awsCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));
    }

    @Test
    void getRegionOrDefault_returns_null_if_AwsCredentialsConfig_region_is_null() {
        assertThat(createObjectUnderTest(awsCredentialsConfig).getRegionOrDefault(), nullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-2", "eu-west-3", "ap-northeast-1"})
    void getRegionOrDefault_returns_Region_of(String regionString) {
        when(awsCredentialsConfig.getRegion()).thenReturn(regionString);

        Region region = Region.of(regionString);
        assertThat(createObjectUnderTest(awsCredentialsConfig).getRegionOrDefault(), equalTo(region));
    }

    @Test
    void getRegionOrDefault_returns_null_if_no_awsCredentialsConfig() {
        assertThat(createObjectUnderTest(null).getRegionOrDefault(), nullValue());
    }

    @Test
    void getRegionOrDefault_returns_null_if_no_region_in_awsCredentialsConfig() {
        assertThat(createObjectUnderTest(awsCredentialsConfig).getRegionOrDefault(), nullValue());
    }
}