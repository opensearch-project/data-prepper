/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.sqssource.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.sqssource.config.QueuesOptions;
import org.opensearch.dataprepper.plugins.source.sqssource.config.SqsSourceConfig;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SqsSourceConfigTest {

    private static final String SQS_CONFIGURATION_YAML = "/src/test/resources/pipeline.yaml";

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void sqs_source_configuration_test() throws IOException {
        final byte[] bytes = Files.readAllBytes(Path.of(Paths.get("").toAbsolutePath() + SQS_CONFIGURATION_YAML));
        final SqsSourceConfig sqsSourceConfig = objectMapper.readValue(bytes, SqsSourceConfig.class);
        final QueuesOptions queues = sqsSourceConfig.getQueues();
        final AwsAuthenticationOptions aws = sqsSourceConfig.getAws();

        assertThat(sqsSourceConfig.getAcknowledgements(),equalTo(false));
        assertThat(queues.getBatchSize(),equalTo(10));
        assertThat(queues.getNumberOfThreads(),equalTo(5));
        assertThat(queues.getUrls().get(0),equalTo("https://sqs.us-east-1.amazonaws.com/123099425585/dp"));

        assertThat(aws.getAwsRegion(),equalTo(Region.US_EAST_1));
        assertThat(aws.getAwsStsRoleArn(),equalTo("arn:aws:iam::278936200144:role/aos-role"));
        assertThat(aws.getAwsStsHeaderOverrides(),nullValue());
    }
}
