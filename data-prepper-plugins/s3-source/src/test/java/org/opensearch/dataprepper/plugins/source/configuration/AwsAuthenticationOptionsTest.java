/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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
        try(final MockedStatic<Region> regionMockedStatic = mockStatic(Region.class)) {
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