/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.configuration.SqsOptions;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.opensearch.dataprepper.plugins.source.SqsDynamicVisibilityTimeoutManager.MAX_VISIBILITY_TIMEOUT_SECONDS;
import static org.opensearch.dataprepper.plugins.source.SqsDynamicVisibilityTimeoutManager.MINIMUM_WAIT_BEFORE_CHANGE_VISIBILITY_TIMEOUT_SECONDS;
import static org.opensearch.dataprepper.plugins.source.SqsDynamicVisibilityTimeoutManager.QUARTER_GB_BYTES;
import static org.opensearch.dataprepper.plugins.source.SqsDynamicVisibilityTimeoutManager.TIME_BETWEEN_CHANGE_MESSAGE_CALL_AND_VISIBILITY_TIMEOUT_SECONDS;
import static org.opensearch.dataprepper.plugins.source.SqsDynamicVisibilityTimeoutManager.VISIBILITY_TIMEOUT_SECONDS_PER_QUARTER_GB;

@ExtendWith(MockitoExtension.class)
public class SqsDynamicVisibilityTimeoutManagerTest {

    @Mock
    private SqsClient sqsClient;

    @Mock
    private SqsOptions sqsOptions;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    @BeforeEach
    void setup() {
        final Random random = new Random();

        final int maxMessages = random.nextInt(10) + 1;

        given(sqsOptions.getMaximumMessages()).willReturn(maxMessages);
        given(sqsOptions.getVisibilityTimeout()).willReturn(Duration.ofSeconds(random.nextInt(MAX_VISIBILITY_TIMEOUT_SECONDS) + 1));
        given(sqsOptions.getSqsUrl()).willReturn(UUID.randomUUID().toString());
    }

    private SqsDynamicVisibilityTimeoutManager createObjectUnderTest() {

        try (MockedStatic<Executors> mockedStatic = mockStatic(Executors.class)) {
            mockedStatic.when(() -> Executors.newScheduledThreadPool(sqsOptions.getMaximumMessages())).thenReturn(scheduledExecutorService);
            return new SqsDynamicVisibilityTimeoutManager(sqsClient, sqsOptions);
        }
    }

    @Test
    void startAndStopDynamicVisibilityForMessageSchedulesCorrectlyAndCancelsExecutionOnStopForAllMessages() {

        final SqsDynamicVisibilityTimeoutManager objectUnderTest = createObjectUnderTest();

        final Random random = new Random();

        final InOrder inOrder = Mockito.inOrder(scheduledExecutorService);
        final InOrder inOrderSqsClient = Mockito.inOrder(sqsClient);

        for (int i = 0; i < sqsOptions.getMaximumMessages(); i++) {
            final Message mockMessage = mock(Message.class);
            given(mockMessage.receiptHandle()).willReturn(UUID.randomUUID().toString());

            final ArgumentCaptor<Runnable> argumentCaptorForChangeMessageIncreaseFunction = ArgumentCaptor.forClass(Runnable.class);
            final ArgumentCaptor<Long> argumentCaptorForInitialWait = ArgumentCaptor.forClass(Long.class);
            final ArgumentCaptor<Long> argumentCaptorForVisibilityIncrease = ArgumentCaptor.forClass(Long.class);

            final int objectSizeBytes = random.nextInt(1_000_000_000);

            final ScheduledFuture mockFuture = mock(ScheduledFuture.class);

            given(scheduledExecutorService.scheduleAtFixedRate(any(), anyLong(), anyLong(), eq(TimeUnit.SECONDS)))
                    .willReturn(mockFuture);

            given(mockFuture.cancel(true)).willReturn(true);

            objectUnderTest.startDynamicVisibilityForMessage(mockMessage, objectSizeBytes);
            inOrder.verify(scheduledExecutorService).scheduleAtFixedRate(argumentCaptorForChangeMessageIncreaseFunction.capture(),
                    argumentCaptorForInitialWait.capture(),
                    argumentCaptorForVisibilityIncrease.capture(),
                    eq(TimeUnit.SECONDS));

            final Runnable changeMessageRunnable = argumentCaptorForChangeMessageIncreaseFunction.getValue();
            final Long initialWait = argumentCaptorForInitialWait.getValue();
            final Long visibilityTimeoutIncreaseRate = argumentCaptorForVisibilityIncrease.getValue();

            assertThat(visibilityTimeoutIncreaseRate, equalTo(getExpectedVisibilityChangeWaitTime(objectSizeBytes)));
            assertThat(initialWait, equalTo(getExpectedInitialWaitTime()));

            final ArgumentCaptor<ChangeMessageVisibilityRequest> requestArgumentCaptor = ArgumentCaptor.forClass(ChangeMessageVisibilityRequest.class);

            changeMessageRunnable.run();

            inOrderSqsClient.verify(sqsClient).changeMessageVisibility(requestArgumentCaptor.capture());

            final ChangeMessageVisibilityRequest changeMessageVisibilityRequest = requestArgumentCaptor.getValue();
            assertThat(changeMessageVisibilityRequest.visibilityTimeout(), equalTo(getExpectedChangeMessageVisibilityTimeout(objectSizeBytes)));
            assertThat(changeMessageVisibilityRequest.queueUrl(), equalTo(sqsOptions.getSqsUrl()));
            assertThat(changeMessageVisibilityRequest.receiptHandle(), equalTo(mockMessage.receiptHandle()));

            objectUnderTest.stopDynamicVisibilityForMessage(mockMessage.messageId());

        }
    }

    private Long getExpectedVisibilityChangeWaitTime(final long objectSizeBytes) {
        final int visibilityTimeoutSeconds = (int) Math.ceil((double) objectSizeBytes / QUARTER_GB_BYTES) * VISIBILITY_TIMEOUT_SECONDS_PER_QUARTER_GB;

        final int visibilityTimeoutChange = Math.min(visibilityTimeoutSeconds, MAX_VISIBILITY_TIMEOUT_SECONDS);

        return (long) Math.min(MINIMUM_WAIT_BEFORE_CHANGE_VISIBILITY_TIMEOUT_SECONDS,
                visibilityTimeoutChange - TIME_BETWEEN_CHANGE_MESSAGE_CALL_AND_VISIBILITY_TIMEOUT_SECONDS);
    }

    private int getExpectedChangeMessageVisibilityTimeout(final long objectSizeBytes) {
        final int visibilityTimeoutSeconds = (int) Math.ceil((double) objectSizeBytes / QUARTER_GB_BYTES) * VISIBILITY_TIMEOUT_SECONDS_PER_QUARTER_GB;

        return Math.min(visibilityTimeoutSeconds, MAX_VISIBILITY_TIMEOUT_SECONDS);
    }

    private Long getExpectedInitialWaitTime() {
        return Math.max(MINIMUM_WAIT_BEFORE_CHANGE_VISIBILITY_TIMEOUT_SECONDS,
                sqsOptions.getVisibilityTimeout().getSeconds() - TIME_BETWEEN_CHANGE_MESSAGE_CALL_AND_VISIBILITY_TIMEOUT_SECONDS);
    }

}
