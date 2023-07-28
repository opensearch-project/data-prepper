/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void GIVEN_valid_regions_WHEN_deserialized_SHOULD_return_regions_as_valid_strings(final String regionString) {
        final Region expectedRegionObject = Region.of(regionString);
        final Map<String, Object> jsonMap = Map.of("region", regionString);
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsRegion(), equalTo(expectedRegionObject));
    }

    @Test
    void GIVEN_no_region_WHEN_deserialized_SHOULD_return_region_as_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsRegion(), nullValue());
    }

    @Test
    void GIVEN_valid_sts_role_arn_WHEN_deserialized_SHOULD_return_as_string() {
        final String stsRoleArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_role_arn", stsRoleArn);
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void GIVEN_empty_sts_role_arn_WHEN_deserialized_SHOULD_return_as_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), nullValue());
    }

    @Test
    void GIVEN_valid_aws_sts_external_id_WHEN_deserialized_SHOULD_return_as_string() {
        final String stsExternalId = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_external_id", stsExternalId);
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), equalTo(stsExternalId));
    }

    @Test
    void GIVEN_valid_aws_sts_external_id_WHEN_deserialized_SHOULD_return_as_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), nullValue());
    }

    @Test
    void GIVEN_valid_aws_sts_header_overrides_WHEN_deserialized_SHOULD_return_as_string_map() {
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Map<String, Object> jsonMap = Map.of("sts_header_overrides", stsHeaderOverrides);
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void GIVEN_valid_aws_sts_header_overrides_WHEN_deserialized_SHOULD_return_as_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsConfig.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), nullValue());
    }
}
