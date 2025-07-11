/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.regions.Region;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AwsStsConfigurationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ParameterizedTest
    @MethodSource("getRegions")
    void testStsConfiguration(final Region region) throws JsonProcessingException {

        final String defaultConfigurationAsString = "{\"region\": \"" + region.toString() + "\", \"sts_role_arn\": \"arn:aws:iam::123456789012:role/test-role\"}";

        final AwsStsConfiguration objectUnderTest = OBJECT_MAPPER.readValue(defaultConfigurationAsString, AwsStsConfiguration.class);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getAwsStsRoleArn(), equalTo("arn:aws:iam::123456789012:role/test-role"));
        assertThat(objectUnderTest.getAwsRegion(), equalTo(region));
    }

    private static List<Region> getRegions() {
        return Region.regions();
    }
}
