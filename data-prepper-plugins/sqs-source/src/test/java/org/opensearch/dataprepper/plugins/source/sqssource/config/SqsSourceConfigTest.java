/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class SqsSourceConfigTest {

    private static final String SQS_CONFIGURATION_YAML = "/src/test/resources/pipeline.yaml";

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void sqs_source_configuration_test() throws IOException {
        final byte[] bytes = Files.readAllBytes(Path.of(Paths.get("").toAbsolutePath() + SQS_CONFIGURATION_YAML));
        final SqsSourceConfig sqsSourceConfig = objectMapper.readValue(bytes, SqsSourceConfig.class);
        final AwsAuthenticationOptions aws = sqsSourceConfig.getAws();

        assertThat(sqsSourceConfig.getAcknowledgements(),equalTo(false));
        assertThat(sqsSourceConfig.getPollingFrequency(),equalTo(Duration.ZERO));
        assertThat(sqsSourceConfig.getBatchSize(),equalTo(10));
        assertThat(sqsSourceConfig.getNumberOfThreads(),equalTo(5));
        assertThat(sqsSourceConfig.getVisibilityTimeout(),nullValue());
        assertThat(sqsSourceConfig.getWaitTime(),nullValue());
        assertThat(sqsSourceConfig.getUrls().get(0),equalTo("https://sqs.us-east-1.amazonaws.com/123099425585/dp"));

        assertThat(aws.getAwsRegion(),equalTo(Region.US_EAST_1));
        assertThat(aws.getAwsStsRoleArn(),equalTo("arn:aws:iam::278936200144:role/aos-role"));
        assertThat(aws.getAwsStsHeaderOverrides().get("test"),equalTo("test"));
    }
}
