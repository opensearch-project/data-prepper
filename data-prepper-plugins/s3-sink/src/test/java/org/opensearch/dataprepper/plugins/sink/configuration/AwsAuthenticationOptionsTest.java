/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.regions.Region;

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
        ReflectivelySetField.setField(AwsAuthenticationOptions.class, awsAuthenticationOptions, "awsRegion", regionString);
        final Region actualRegion;
        try (final MockedStatic<Region> regionMockedStatic = mockStatic(Region.class)) {
            regionMockedStatic.when(() -> Region.of(regionString)).thenReturn(expectedRegionObject);
            actualRegion = awsAuthenticationOptions.getAwsRegion();
        }
        assertThat(actualRegion, equalTo(expectedRegionObject));
    }

    @Test
    void getAwsRegion_returns_null_when_region_is_null() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(AwsAuthenticationOptions.class, awsAuthenticationOptions, "awsRegion", null);
        assertThat(awsAuthenticationOptions.getAwsRegion(), nullValue());
    }
}