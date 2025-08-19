/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.regions.Region;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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

    @Test
    void testStsConfigurationWithHeaderOverrides() throws JsonProcessingException {
        final String configWithHeaderOverrides =
                "{\"region\": \"us-west-2\", " +
                        "\"sts_role_arn\": \"arn:aws:iam::123456789012:role/test-role\", " +
                        "\"sts_header_overrides\": {\"header1\": \"value1\", \"header2\": \"value2\"}}";

        final AwsStsConfiguration objectUnderTest = OBJECT_MAPPER.readValue(configWithHeaderOverrides, AwsStsConfiguration.class);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getAwsStsRoleArn(), equalTo("arn:aws:iam::123456789012:role/test-role"));
        assertThat(objectUnderTest.getAwsRegion(), equalTo(Region.US_WEST_2));
        assertThat(objectUnderTest.getStsHeaderOverrides(), notNullValue());
        assertThat(objectUnderTest.getStsHeaderOverrides().size(), equalTo(2));
        assertThat(objectUnderTest.getStsHeaderOverrides().get("header1"), equalTo("value1"));
        assertThat(objectUnderTest.getStsHeaderOverrides().get("header2"), equalTo("value2"));
    }

    @Test
    void testStsConfigurationWithoutHeaderOverrides() throws JsonProcessingException {
        final String configWithoutHeaderOverrides =
                "{\"region\": \"us-west-2\", " +
                        "\"sts_role_arn\": \"arn:aws:iam::123456789012:role/test-role\"}";

        final AwsStsConfiguration objectUnderTest = OBJECT_MAPPER.readValue(configWithoutHeaderOverrides, AwsStsConfiguration.class);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getStsHeaderOverrides(), nullValue());
    }

    private static List<Region> getRegions() {
        return Region.regions();
    }
}
