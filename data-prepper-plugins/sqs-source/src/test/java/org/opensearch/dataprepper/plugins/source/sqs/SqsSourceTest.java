/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsSourceTest {
    private final String PLUGIN_NAME = "sqs";
    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private SqsSource sqsSource;
    private PluginMetrics pluginMetrics;
    private SqsSourceConfig sqsSourceConfig;
    private AcknowledgementSetManager acknowledgementSetManager;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private Buffer<Record<Event>> buffer;


    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        sqsSourceConfig = mock(SqsSourceConfig.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        sqsSource = new SqsSource(pluginMetrics, sqsSourceConfig, acknowledgementSetManager, awsCredentialsSupplier);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        assertThrows(IllegalStateException.class, () -> sqsSource.start(null));
    }

    @Test
    void start_should_not_throw_when_buffer_is_not_null() {
        Buffer<Record<Event>> buffer = mock(Buffer.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/example-role");
        when(sqsSourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(mock(AwsCredentialsProvider.class));
        assertDoesNotThrow(() -> sqsSource.start(buffer));
    }

    @Test
    void stop_should_not_throw_when_sqsService_is_null() {
        sqsSource.stop();
    }
}