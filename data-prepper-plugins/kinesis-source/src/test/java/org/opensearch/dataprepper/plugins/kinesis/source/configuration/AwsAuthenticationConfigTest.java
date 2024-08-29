package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AwsAuthenticationConfigTest {
    private ObjectMapper objectMapper = new ObjectMapper();

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void getAwsRegionReturnsRegion(final String regionString) {
        final Region expectedRegionObject = Region.of(regionString);
        final Map<String, Object> jsonMap = Map.of("region", regionString);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsRegion(), equalTo(expectedRegionObject));
    }

    @Test
    void getAwsRegionReturnsNullWhenRegionIsNull() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsRegion(), nullValue());
    }

    @Test
    void getAwsStsRoleArnReturnsValueFromDeserializedJSON() {
        final String stsRoleArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_role_arn", stsRoleArn);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void getAwsStsRoleArnReturnsNullIfNotInJSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), nullValue());
    }

    @Test
    void getAwsStsExternalIdReturnsValueFromDeserializedJSON() {
        final String stsExternalId = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_external_id", stsExternalId);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), equalTo(stsExternalId));
    }

    @Test
    void getAwsStsExternalIdReturnsNullIfNotInJSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), nullValue());
    }

    @Test
    void getAwsStsHeaderOverridesReturnsValueFromDeserializedJSON() {
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Map<String, Object> jsonMap = Map.of("sts_header_overrides", stsHeaderOverrides);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void getAwsStsHeaderOverridesReturnsNullIfNotInJSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), nullValue());
    }
}
