/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.s3.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationSourceOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.configuration.SqsOptions;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsClientFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsServiceTest {
    private static final Duration SIMULATED_SINK_DELAY = Duration.ofMillis(750);

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private S3Service s3Service;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;
    @Mock
    private SqsOptions sqsOptions;
    @Mock
    private SqsClient sqsClient;

    private MockedStatic<SqsClientFactory> sqsClientFactory;
    private ExecutorService simulatedSink;
    private List<Consumer<Boolean>> acknowledgementCallbacks;
    private AtomicInteger messagesToDeliver;

    @BeforeEach
    void setUp() {
        sqsClientFactory = mockStatic(SqsClientFactory.class);
        sqsClientFactory.when(() -> SqsClientFactory.createSqsClient(any(), any())).thenReturn(sqsClient);
        simulatedSink = Executors.newCachedThreadPool();
        acknowledgementCallbacks = new CopyOnWriteArrayList<>();
        messagesToDeliver = new AtomicInteger(1);

        when(s3SourceConfig.getNumWorkers()).thenReturn(1);
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_2);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);
        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
        when(s3SourceConfig.getNotificationSource()).thenReturn(NotificationSourceOption.S3);
        when(s3SourceConfig.getDataSelection()).thenReturn(S3DataSelection.DATA_AND_METADATA);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs.us-east-2.amazonaws.com/123456789012/" + UUID.randomUUID());
        when(sqsOptions.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(30));
        when(sqsOptions.getVisibilityDuplicateProtectionTimeout()).thenReturn(Duration.ofHours(2));
        when(sqsOptions.getVisibilityDuplicateProtection()).thenReturn(false);
        when(sqsOptions.getPollDelay()).thenReturn(Duration.ZERO);
        when(sqsOptions.getMaximumMessages()).thenReturn(10);
        when(sqsOptions.getWaitTime()).thenReturn(Duration.ZERO);
        when(sqsOptions.getShutdownTimeout()).thenReturn(Duration.ofSeconds(30));

        when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(pluginMetrics.timer(anyString())).thenReturn(mock(Timer.class));

        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenAnswer(invocation -> {
            acknowledgementCallbacks.add(invocation.getArgument(0));
            return acknowledgementSet;
        });

        // Each worker receives one S3 event notification. Further polls find an empty queue.
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
            if (messagesToDeliver.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                return ReceiveMessageResponse.builder().messages(createMessage()).build();
            }
            Thread.sleep(10);
            return ReceiveMessageResponse.builder().build();
        });
        lenient().when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
            final DeleteMessageBatchRequest request = invocation.getArgument(0);
            return DeleteMessageBatchResponse.builder()
                    .successful(request.entries().stream()
                            .map(entry -> DeleteMessageBatchResultEntry.builder().id(entry.id()).build())
                            .toArray(DeleteMessageBatchResultEntry[]::new))
                    .build();
        });
    }

    @AfterEach
    void tearDown() {
        simulatedSink.shutdownNow();
        sqsClientFactory.close();
    }

    private SqsService createObjectUnderTest() {
        return new SqsService(acknowledgementSetManager, s3SourceConfig, s3Service, pluginMetrics, awsCredentialsProvider);
    }

    private void startAndWaitForObjectsToBeSentToBuffer(final SqsService objectUnderTest, final int expectedAcknowledgementSets) throws IOException {
        objectUnderTest.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            verify(s3Service, times(expectedAcknowledgementSets)).addS3Object(any(), eq(S3DataSelection.DATA_AND_METADATA), eq(acknowledgementSet));
            verify(acknowledgementSet, times(expectedAcknowledgementSets)).complete();
        });
    }

    @Test
    void stop_waits_for_pending_acknowledgements_and_deletes_the_acknowledged_message_before_closing_the_client() throws Exception {
        final SqsService objectUnderTest = createObjectUnderTest();
        startAndWaitForObjectsToBeSentToBuffer(objectUnderTest, 1);

        final AtomicBoolean acknowledged = new AtomicBoolean(false);
        simulatedSink.submit(() -> {
            Thread.sleep(SIMULATED_SINK_DELAY.toMillis());
            acknowledged.set(true);
            acknowledgementCallbacks.get(0).accept(true);
            return null;
        });

        objectUnderTest.stop();

        assertThat(acknowledged.get(), equalTo(true));
        final InOrder inOrder = inOrder(sqsClient);
        inOrder.verify(sqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        inOrder.verify(sqsClient).close();
    }

    @Test
    void stop_waits_for_pending_acknowledgements_of_every_worker() throws Exception {
        when(s3SourceConfig.getNumWorkers()).thenReturn(2);
        messagesToDeliver.set(2);
        final SqsService objectUnderTest = createObjectUnderTest();
        startAndWaitForObjectsToBeSentToBuffer(objectUnderTest, 2);

        final AtomicInteger acknowledged = new AtomicInteger(0);
        for (int i = 0; i < 2; i++) {
            final Consumer<Boolean> callback = acknowledgementCallbacks.get(i);
            simulatedSink.submit(() -> {
                Thread.sleep(SIMULATED_SINK_DELAY.toMillis());
                acknowledged.incrementAndGet();
                callback.accept(true);
                return null;
            });
        }

        objectUnderTest.stop();

        assertThat(acknowledged.get(), equalTo(2));
        verify(sqsClient, times(2)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsClient).close();
    }

    @Test
    void stop_does_not_wait_longer_than_the_shutdown_timeout_for_pending_acknowledgements() throws Exception {
        when(sqsOptions.getShutdownTimeout()).thenReturn(Duration.ofSeconds(1));
        final SqsService objectUnderTest = createObjectUnderTest();
        startAndWaitForObjectsToBeSentToBuffer(objectUnderTest, 1);

        final long startNanos = System.nanoTime();
        objectUnderTest.stop();
        final Duration stopDuration = Duration.ofNanos(System.nanoTime() - startNanos);

        assertThat(stopDuration, greaterThanOrEqualTo(Duration.ofMillis(900)));
        assertThat(stopDuration, lessThan(Duration.ofSeconds(10)));
        verify(sqsClient).close();
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void acknowledgement_received_after_stop_has_timed_out_does_not_call_SQS_with_the_closed_client() throws Exception {
        when(sqsOptions.getShutdownTimeout()).thenReturn(Duration.ofSeconds(1));
        final SqsService objectUnderTest = createObjectUnderTest();
        startAndWaitForObjectsToBeSentToBuffer(objectUnderTest, 1);

        objectUnderTest.stop();
        acknowledgementCallbacks.get(0).accept(true);

        verify(sqsClient).close();
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void stop_does_not_wait_when_all_acknowledgements_have_already_completed() throws Exception {
        final SqsService objectUnderTest = createObjectUnderTest();
        startAndWaitForObjectsToBeSentToBuffer(objectUnderTest, 1);
        acknowledgementCallbacks.get(0).accept(true);

        final long startNanos = System.nanoTime();
        objectUnderTest.stop();
        final Duration stopDuration = Duration.ofNanos(System.nanoTime() - startNanos);

        assertThat(stopDuration, lessThan(Duration.ofSeconds(10)));
        verify(sqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsClient).close();
    }

    private static Message createMessage() {
        final String id = UUID.randomUUID().toString();
        return Message.builder()
                .messageId(id)
                .receiptHandle(id)
                .body(createEventNotification(Instant.now()))
                .build();
    }

    private static String createEventNotification(final Instant eventTime) {
        return "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\"," +
                "\"eventTime\":\"" + eventTime + "\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROAX:xxxxxx\"}," +
                "\"requestParameters\":{\"sourceIPAddress\":\"99.99.999.99\"},\"responseElements\":{\"x-amz-request-id\":\"ABCD\"," +
                "\"x-amz-id-2\":\"abcd\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"s3SourceEventNotification\"," +
                "\"bucket\":{\"name\":\"bucketName\",\"ownerIdentity\":{\"principalId\":\"ID\"},\"arn\":\"arn:aws:s3:::bucketName\"}," +
                "\"object\":{\"key\":\"File.gz\",\"size\":72,\"eTag\":\"abcd\",\"sequencer\":\"ABCD\"}}}]}";
    }
}
