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
import org.mockito.MockedStatic;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class AwsAuthenticationOptionsTest {

    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        awsAuthenticationOptions = new AwsAuthenticationOptions();
    }

    @Test
    void getAwsRegion_returns_Region_of() throws NoSuchFieldException, IllegalAccessException {
        final String regionString = UUID.randomUUID().toString();
        final Region expectedRegionObject = mock(Region.class);
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", regionString);
        final Region actualRegion;
        try (final MockedStatic<Region> regionMockedStatic = mockStatic(Region.class)) {
            regionMockedStatic.when(() -> Region.of(regionString)).thenReturn(expectedRegionObject);
            actualRegion = awsAuthenticationOptions.getAwsRegion();
        }
        assertThat(actualRegion, equalTo(expectedRegionObject));
    }

    @Test
    void getAwsRegion_returns_null_when_region_is_null() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", null);
        assertThat(awsAuthenticationOptions.getAwsRegion(), nullValue());
    }

    @Test
    void getStsExternalId_notNull() throws NoSuchFieldException, IllegalAccessException {
        final String externalId = UUID.randomUUID().toString();
        reflectivelySetField(awsAuthenticationOptions, "awsStsExternalId", externalId);
        assertThat(awsAuthenticationOptions.getAwsStsExternalId(), equalTo(externalId));
    }

    @Test
    void getStsExternalId_Null() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsStsExternalId", null);
        assertThat(awsAuthenticationOptions.getAwsStsExternalId(), nullValue());
    }

    @Test
    void getAwsStsRoleArn_returns_correct_arn() throws NoSuchFieldException, IllegalAccessException {
        final String stsRoleArn = "arn:aws:iam::123456789012:role/SampleRole";
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", stsRoleArn);
        assertThat(awsAuthenticationOptions.getAwsStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void getAwsStsHeaderOverrides_returns_correct_map() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> headerOverrides = new HashMap<>();
        headerOverrides.put("header1", "value1");
        headerOverrides.put("header2", "value2");
        reflectivelySetField(awsAuthenticationOptions, "awsStsHeaderOverrides", headerOverrides);
        assertThat(awsAuthenticationOptions.getAwsStsHeaderOverrides(), equalTo(headerOverrides));
    }

    @Test
    void validateStsRoleArn_with_invalid_format_throws_exception() throws NoSuchFieldException, IllegalAccessException {
        final String invalidFormatArn = "invalid-arn-format";
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", invalidFormatArn);

        try (final MockedStatic<Arn> arnMockedStatic = mockStatic(Arn.class)) {
            arnMockedStatic.when(() -> Arn.fromString(invalidFormatArn))
                    .thenThrow(new IllegalArgumentException("The value provided for sts_role_arn is not a valid AWS ARN. Provided value: " + invalidFormatArn));

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                awsAuthenticationOptions.validateStsRoleArn();
            });
            assertThat(exception.getMessage(), equalTo("The value provided for sts_role_arn is not a valid AWS ARN. Provided value: " + invalidFormatArn));
        }
    }

    @Test
    void validateStsRoleArn_does_not_throw_for_valid_role_Arn() throws NoSuchFieldException, IllegalAccessException {
        final String validRoleArn = "arn:aws:iam::123456789012:role/SampleRole";
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", validRoleArn);
        try {
            awsAuthenticationOptions.validateStsRoleArn();
        } catch (Exception e) {
            throw new AssertionError("Exception should not be thrown for a valid role ARN", e);
        }
    }

    @Test
    void validateStsRoleArn_throws_exception_for_non_role_resource() throws NoSuchFieldException, IllegalAccessException {
        final String nonRoleResourceArn = "arn:aws:iam::123456789012:group/MyGroup";
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", nonRoleResourceArn);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> awsAuthenticationOptions.validateStsRoleArn());
        assertThat(exception.getMessage(), equalTo("sts_role_arn must be an IAM Role"));
    }

    @Test
    void validateStsRoleArn_throws_exception_when_service_is_not_iam() throws NoSuchFieldException, IllegalAccessException {
        final String invalidServiceArn = "arn:aws:s3::123456789012:role/SampleRole";
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", invalidServiceArn);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> awsAuthenticationOptions.validateStsRoleArn());
        assertThat(exception.getMessage(), equalTo("sts_role_arn must be an IAM Role"));
    }


    private void reflectivelySetField(final AwsAuthenticationOptions awsAuthenticationOptions, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = AwsAuthenticationOptions.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(awsAuthenticationOptions, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
