/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CredentialsIdentifierTest {
    @Mock
    private AwsCredentialsOptions credentialsOptions;

    @Mock
    private AwsCredentialsOptions otherCredentialsOptions;

    @Test
    void equals_on_null_other() {
        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);

        assertThat(objectUnderTest.equals(null), equalTo(false));
    }

    @Test
    void equals_hashCode_on_same_instance() {
        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);

        assertThat(objectUnderTest.equals(objectUnderTest), equalTo(true));
        assertThat(objectUnderTest.hashCode(), equalTo(objectUnderTest.hashCode()));
    }

    @Test
    void equals_hashCode_on_empty_objects() {
        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(true));
        assertThat(objectUnderTest.hashCode(), equalTo(other.hashCode()));
    }

    @Test
    void equals_hashCode_on_equal_objects() {
        final String stsRoleArn = UUID.randomUUID().toString();
        when(credentialsOptions.getStsRoleArn()).thenReturn(stsRoleArn);
        when(otherCredentialsOptions.getStsRoleArn()).thenReturn(stsRoleArn);
        when(credentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
        when(otherCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
        final String headerName = UUID.randomUUID().toString();
        final String headerValue = UUID.randomUUID().toString();
        when(credentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.singletonMap(headerName, headerValue));
        when(otherCredentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.singletonMap(headerName, headerValue));

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(true));
        assertThat(objectUnderTest.hashCode(), equalTo(other.hashCode()));
    }

    @Test
    void equals_hashCode_on_non_equal_objects() {
        when(credentialsOptions.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(otherCredentialsOptions.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(credentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
        when(otherCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
        final String headerName = UUID.randomUUID().toString();
        final String headerValue = UUID.randomUUID().toString();
        when(credentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.singletonMap(headerName, headerValue));
        when(otherCredentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.singletonMap(headerName, headerValue));

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(false));
        assertThat(objectUnderTest.hashCode(), not(equalTo(other.hashCode())));
    }

    @Test
    void equals_hashCode_on_equal_StsRoleArn() {

        final String stsRoleArn = UUID.randomUUID().toString();
        when(credentialsOptions.getStsRoleArn()).thenReturn(stsRoleArn);
        when(otherCredentialsOptions.getStsRoleArn()).thenReturn(stsRoleArn);

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(true));
        assertThat(objectUnderTest.hashCode(), equalTo(other.hashCode()));
    }

    @Test
    void equals_hashCode_on_non_equal_StsRoleArn() {

        when(credentialsOptions.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(otherCredentialsOptions.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(false));
        assertThat(objectUnderTest.hashCode(), not(equalTo(other.hashCode())));
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "eu-central-1"})
    void equals_hashCode_on_equal_regions(final String regionString) {

        final Region region = Region.of(regionString);
        when(credentialsOptions.getRegion()).thenReturn(region);
        when(otherCredentialsOptions.getRegion()).thenReturn(region);

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(true));
        assertThat(objectUnderTest.hashCode(), equalTo(other.hashCode()));
    }

    @Test
    void equals_hashCode_on_non_equal_regions() {
        when(credentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
        when(otherCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_2);

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(false));
        assertThat(objectUnderTest.hashCode(), not(equalTo(other.hashCode())));
    }

    @Test
    void equals_hashCode_on_equal_StsHeaderOverrides() {
        when(credentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(otherCredentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.emptyMap());

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(true));
        assertThat(objectUnderTest.hashCode(), equalTo(other.hashCode()));
    }

    @Test
    void equals_hashCode_on_non_equal_StsHeaderOverrides() {
        when(credentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        when(otherCredentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.emptyMap());

        final CredentialsIdentifier objectUnderTest = CredentialsIdentifier.fromAwsCredentialsOption(credentialsOptions);
        final CredentialsIdentifier other = CredentialsIdentifier.fromAwsCredentialsOption(otherCredentialsOptions);

        assertThat(objectUnderTest.equals(other), equalTo(false));
        assertThat(objectUnderTest.hashCode(), not(equalTo(other.hashCode())));
    }
}