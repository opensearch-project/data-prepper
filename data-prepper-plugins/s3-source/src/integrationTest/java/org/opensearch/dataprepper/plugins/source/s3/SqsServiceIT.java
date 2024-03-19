/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationSourceOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.SqsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsServiceIT {
    private SqsClient sqsClient;
    private S3Service s3Service;
    @Mock
    private SqsOptions sqsOptions;
    private S3SourceConfig s3SourceConfig;
    private PluginMetrics pluginMetrics;
    private S3ObjectGenerator s3ObjectGenerator;
    private String bucket;
    private AcknowledgementSetManager acknowledgementSetManager;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private AtomicInteger deletedCount;
    private Counter deletedCounter;
    private Counter numMessagesCounter;
    private AtomicInteger numMessages;

    @BeforeEach
    void setUp() {
        String receivedMessages = SqsWorker.SQS_MESSAGES_RECEIVED_METRIC_NAME;
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        final S3Client s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        bucket = System.getProperty("tests.s3source.bucket");
        s3ObjectGenerator = new S3ObjectGenerator(s3Client, bucket);
        s3Service = mock(S3Service.class);

        sqsClient = SqsClient.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();

        deletedCount = new AtomicInteger(0);
        numMessages = new AtomicInteger(0);
        pluginMetrics = mock(PluginMetrics.class);
        final DistributionSummary distributionSummary = mock(DistributionSummary.class);
        final Timer sqsMessageDelayTimer = mock(Timer.class);

        deletedCounter = mock(Counter.class);
        numMessagesCounter = mock(Counter.class);
        lenient().when(pluginMetrics.counter(SqsWorker.SQS_MESSAGES_RECEIVED_METRIC_NAME)).thenReturn(numMessagesCounter);
        lenient().when(pluginMetrics.counter(SqsWorker.SQS_MESSAGES_DELETED_METRIC_NAME)).thenReturn(deletedCounter);
        lenient().when(pluginMetrics.counter(SqsWorker.S3_OBJECTS_EMPTY_METRIC_NAME)).thenReturn(mock(Counter.class));
        lenient().when(pluginMetrics.summary(anyString())).thenReturn(distributionSummary);
        when(pluginMetrics.timer(anyString())).thenReturn(sqsMessageDelayTimer);
        lenient().doAnswer((val) -> {
            int x = numMessages.addAndGet(((Double)val.getArgument(0)).intValue());
            return null;
        }).when(numMessagesCounter).increment(any(Double.class));

        s3SourceConfig = mock(S3SourceConfig.class);
        sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getSqsUrl()).thenReturn(System.getProperty("tests.s3source.queue.url"));
        when(sqsOptions.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(60));
        when(sqsOptions.getMaximumMessages()).thenReturn(10);
        when(sqsOptions.getWaitTime()).thenReturn(Duration.ofSeconds(10));
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);
        lenient().when(s3SourceConfig.getOnErrorOption()).thenReturn(OnErrorOption.DELETE_MESSAGES);

        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(System.getProperty("tests.s3source.region")));
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);
        lenient().when(s3SourceConfig.getNotificationSource()).thenReturn(NotificationSourceOption.S3);
 
        // Clear SQS queue messages before running each test
        clearSqsQueue();
        numMessages = new AtomicInteger(0);
    }

    private SqsService createObjectUnderTest() {
        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();
        return new SqsService(acknowledgementSetManager, s3SourceConfig, s3Service, pluginMetrics, awsCredentialsProvider);
    }

    private void writeToS3(final int numberOfObjectsToWrite) throws IOException {
        final int numberOfRecords = 100;
        final NewlineDelimitedRecordsGenerator newlineDelimitedRecordsGenerator = new NewlineDelimitedRecordsGenerator();
        for (int i = 0; i < numberOfObjectsToWrite; i++) {
            final String key = "s3 source/sqs/" + UUID.randomUUID() + "_" + Instant.now().toString() + newlineDelimitedRecordsGenerator.getFileExtension();
            // isCompressionEnabled is set to false since we test for compression in S3ObjectWorkerIT
            s3ObjectGenerator.write(numberOfRecords, key, newlineDelimitedRecordsGenerator, false);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    public void test_sqsService(int numWorkers) throws IOException {
        int numberOfObjectsToWrite = 5;

        when(s3SourceConfig.getNumWorkers()).thenReturn(numWorkers);
        final SqsService objectUnderTest = createObjectUnderTest();
        writeToS3(numberOfObjectsToWrite);
        numMessages.set(0);
        objectUnderTest.start();
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
        assertThat(numMessages.get(), equalTo(numberOfObjectsToWrite));
        });
        final ArgumentCaptor<S3ObjectReference> s3ObjectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        verify(s3Service, atLeastOnce()).addS3Object(s3ObjectReferenceArgumentCaptor.capture(), eq(null));
        assertThat(s3ObjectReferenceArgumentCaptor.getValue().getBucketName(), equalTo(bucket));
        assertThat(s3ObjectReferenceArgumentCaptor.getValue().getKey(), startsWith("s3 source/sqs/"));
        objectUnderTest.stop();
    }

    private void clearSqsQueue() {
        Backoff backoff = Backoff.exponential(SqsService.INITIAL_DELAY, SqsService.MAXIMUM_DELAY).withJitter(SqsService.JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        final SqsWorker sqsWorker = new SqsWorker(acknowledgementSetManager, sqsClient, s3Service, s3SourceConfig, pluginMetrics, backoff);
        //final SqsService objectUnderTest = createObjectUnderTest();
        int sqsMessagesProcessed;
        do {
            sqsMessagesProcessed = sqsWorker.processSqsMessages();
        }
        while (sqsMessagesProcessed > 0);
    }
}

