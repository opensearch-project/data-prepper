/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.NotificationTypeOption;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3SourceTest {
    private final String PLUGIN_NAME = "s3";
    private final String TEST_PIPELINE_NAME = "test_pipeline";

    private S3Source s3Source;
    private BlockingBuffer<Record<Event>> testBuffer;
    private PluginMetrics pluginMetrics;
    private S3SourceConfig s3SourceConfig;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private SqsOptions sqsOptions;


    @BeforeEach
    void setUp() {
        testBuffer = getBuffer();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(null);

        sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getMaximumMessages()).thenReturn(10);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        when(sqsOptions.getThreadCount()).thenReturn(1);
        when(sqsOptions.getPollDelay()).thenReturn(Duration.ofSeconds(10));
        when(sqsOptions.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(10));
        when(sqsOptions.getWaitTime()).thenReturn(Duration.ofSeconds(10));

        s3SourceConfig = mock(S3SourceConfig.class);
        when(s3SourceConfig.getNotificationType()).thenReturn(NotificationTypeOption.SQS);
        when(s3SourceConfig.getAWSAuthentication()).thenReturn(awsAuthenticationOptions);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

        s3Source = new S3Source(pluginMetrics, s3SourceConfig);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        testBuffer = null;
        assertThrows(IllegalStateException.class, () -> s3Source.start(testBuffer));
    }

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap) {{
            setPipelineName(TEST_PIPELINE_NAME);
        }};
        return new BlockingBuffer<>(pluginSetting);
    }
}