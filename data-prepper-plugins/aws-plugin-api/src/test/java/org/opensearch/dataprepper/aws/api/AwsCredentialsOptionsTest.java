/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsCredentialsOptionsTest {
    @Test
    void without_StsRoleArn() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsRoleArn(), nullValue());
    }

    @Test
    void with_StsRoleArn() {
        final String roleArn = "arn:aws:iam::123456789012:role/" + UUID.randomUUID();
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withStsRoleArn(roleArn)
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsRoleArn(), notNullValue());
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(roleArn));
    }

    @Test
    void without_StsExternalId() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsExternalId(), nullValue());
    }

    @Test
    void with_StsExternalId() {
        final String externalId = UUID.randomUUID().toString();
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withStsExternalId(externalId)
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsExternalId(), notNullValue());
        assertThat(awsCredentialsOptions.getStsExternalId(), equalTo(externalId));
    }

    @Test
    void without_Region() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), nullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-west-1"})
    void with_Region_string(final String regionString) {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withRegion(regionString)
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.of(regionString)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-west-1"})
    void with_Region_model(final String regionString) {
        final Region region = Region.of(regionString);

        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withRegion(region)
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(region));
    }

    @Test
    void without_StsHeaderOverrides() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides().size(), equalTo(0));
    }

    @Test
    void with_explicit_null_StsHeaderOverrides() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withStsHeaderOverrides(null)
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides().size(), equalTo(0));
    }

    @Test
    void with_StsHeaderOverrides() {
        final Map<String, String> stsHeaderOverrides = Map.of(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withStsHeaderOverrides(stsHeaderOverrides)
                .build();

        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides().size(), equalTo(stsHeaderOverrides.size()));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void defaultOptions_returns_with_null_or_empty_values() {
        AwsCredentialsOptions defaultOptions = AwsCredentialsOptions.defaultOptions();

        assertThat(defaultOptions, notNullValue());
        assertThat(defaultOptions.getRegion(), nullValue());
        assertThat(defaultOptions.getStsRoleArn(), nullValue());
        assertThat(defaultOptions.getStsExternalId(), nullValue());
        assertThat(defaultOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
    }

    @Test
    void defaultOptions_returns_same_instance_on_multiple_calls() {
        assertThat(AwsCredentialsOptions.defaultOptions(),
                sameInstance(AwsCredentialsOptions.defaultOptions()));
    }
}