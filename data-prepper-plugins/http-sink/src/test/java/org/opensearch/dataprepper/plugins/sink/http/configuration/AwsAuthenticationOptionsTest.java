/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
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

class AwsAuthenticationOptionsTest {
    private ObjectMapper objectMapper = new ObjectMapper();
    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void getAwsRegion_returns_Region_of(final String regionString) {
        final Region expectedRegionObject = Region.of(regionString);
        final Map<String, Object> jsonMap = Map.of("region", regionString);
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsRegion(), equalTo(expectedRegionObject));
    }

    @Test
    void getAwsRegion_returns_null_when_region_is_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsRegion(), nullValue());
    }

    @Test
    void getAwsStsRoleArn_returns_value_from_deserialized_JSON() {
        final String stsRoleArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_role_arn", stsRoleArn);
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void getAwsStsRoleArn_returns_null_if_not_in_JSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), nullValue());
    }

    @Test
    void getAwsStsExternalId_returns_value_from_deserialized_JSON() {
        final String stsExternalId = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_external_id", stsExternalId);
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), equalTo(stsExternalId));
    }

    @Test
    void getAwsStsExternalId_returns_null_if_not_in_JSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), nullValue());
    }

    @Test
    void getAwsStsHeaderOverrides_returns_value_from_deserialized_JSON() {
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Map<String, Object> jsonMap = Map.of("sts_header_overrides", stsHeaderOverrides);
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void getAwsStsHeaderOverrides_returns_null_if_not_in_JSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationOptions objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationOptions.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), nullValue());
    }
}