package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.regions.Region;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class KmsConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void toCredentialsOptions_returns_options_with_correct_values() {
        String roleArn = UUID.randomUUID().toString();
        String region = UUID.randomUUID().toString();
        Map<String, Object> options = Map.of(
                "sts_role_arn", roleArn,
                "region", region
        );

        KmsConfig kmsConfig = objectMapper.convertValue(options, KmsConfig.class);

        AwsCredentialsOptions credentialsOptions = kmsConfig.toCredentialsOptions();
        assertThat(credentialsOptions, notNullValue());
        assertThat(credentialsOptions.getStsRoleArn(), equalTo(roleArn));
        assertThat(credentialsOptions.getRegion(), equalTo(Region.of(region)));
    }
}