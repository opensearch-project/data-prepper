/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.exception.RetransmissionLimitException;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.*;

//TODO: Add Codec session.
//TODO: Finish adding feature for ARN reading.

public class CloudWatchLogsServiceTest {
    private CloudWatchLogsClient mockClient;
    private PutLogEventsResponse putLogEventsResponse;
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private ThresholdCheck thresholdCheck;
    private AwsConfig awsConfig;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private BufferFactory bufferFactory;
    private Buffer buffer;
    private PluginMetrics pluginMetrics;
    private Counter requestSuccessCounter;
    private Counter requestFailCounter;
    private Counter successEventCounter;
    private Counter failedEventCounter;
    private final String TEST_LOG_GROUP = "TESTGROUP";
    private final String TEST_LOG_STREAM = "TESTSTREAM";
    private static final int messageKeyByteSize = 14;
    private static final int convertToBytesFromKiloBytes  = 1024;

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        thresholdCheck = new ThresholdCheck(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSize() * convertToBytesFromKiloBytes,
                thresholdConfig.getMaxRequestSize(), thresholdConfig.getLogSendInterval());

        awsConfig = mock(AwsConfig.class);
        bufferFactory = new InMemoryBufferFactory();
        buffer = bufferFactory.getBuffer();
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        pluginMetrics = mock(PluginMetrics.class);
        requestSuccessCounter = mock(Counter.class);
        requestFailCounter = mock(Counter.class);
        successEventCounter = mock(Counter.class);
        failedEventCounter = mock(Counter.class);

        final String stsRoleArn = UUID.randomUUID().toString();
        final String externalId = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(cloudWatchLogsSinkConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);
        when(cloudWatchLogsSinkConfig.getBufferType()).thenReturn("in_memory");
        when(cloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(awsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsConfig.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        when(awsConfig.getAwsStsExternalId()).thenReturn(externalId);

        lenient().when(pluginMetrics.counter(CloudWatchLogsService.NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS)).thenReturn(successEventCounter);
        lenient().when(pluginMetrics.counter(CloudWatchLogsService.REQUESTS_SUCCEEDED)).thenReturn(requestSuccessCounter);
        lenient().when(pluginMetrics.counter(CloudWatchLogsService.NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL)).thenReturn(failedEventCounter);
        lenient().when(pluginMetrics.counter(CloudWatchLogsService.REQUESTS_FAILED)).thenReturn(requestFailCounter);
    }

    void setThresholdForTestingRequestSize(int size) {
        thresholdCheck = new ThresholdCheck(10000, size, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST, ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME);
    }

    void setThresholdForTestingMaxRequestRequestSize() {
        thresholdCheck = new ThresholdCheck(10000, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST * 2, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST * 2, ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME);
    }

    CloudWatchLogsService getCwlClientWithMemoryBuffer() {
        return new CloudWatchLogsService(mockClient, cloudWatchLogsSinkConfig, buffer, pluginMetrics,
                thresholdCheck, thresholdConfig.getRetryCount(), ThresholdConfig.DEFAULT_BACKOFF_TIME);
    }

    void setMockClientNoErrors() {
        mockClient = mock(CloudWatchLogsClient.class);
        putLogEventsResponse = mock(PutLogEventsResponse.class);
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(putLogEventsResponse);
        when(putLogEventsResponse.rejectedLogEventsInfo()).thenReturn(null);
    }

    void setMockClientThrowCWLException() {
        mockClient = mock(CloudWatchLogsClient.class);
        doThrow(AwsServiceException.class).when(mockClient).putLogEvents(any(PutLogEventsRequest.class));
    }

    Collection<Record<Event>> getSampleRecords(int numberOfRecords) {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsLarge(int numberOfRecords, int sizeOfRecordsBytes) {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        final String testMessage = "a";
        for (int i = 0; i < numberOfRecords; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage(testMessage.repeat(sizeOfRecordsBytes));
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    @Test
    void client_creation_test() {
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();
    }

    @Test
    void retry_count_limit_reached_test() {
        setMockClientThrowCWLException();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        try {
            cloudWatchLogsService.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 2));
        } catch (RetransmissionLimitException e) { //TODO: Create a dedicated RuntimeException for this.
            assertThat(e, notNullValue());
        }
    }

    @Test
    void check_failed_event_transmission_test() {
        setMockClientThrowCWLException();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        try {
            cloudWatchLogsService.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE));
        } catch (RetransmissionLimitException e) {
            verify(failedEventCounter).increment(ThresholdConfig.DEFAULT_BATCH_SIZE);
        }
    }

    @Test
    void check_successful_event_transmission_test() {
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        cloudWatchLogsService.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 2));

        verify(successEventCounter, atLeast(2)).increment(anyDouble());
    }

    @Test
    void check_failed_event_test() {
        setMockClientThrowCWLException();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        try {
            cloudWatchLogsService.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 4));
        } catch (RetransmissionLimitException e) {
            verify(requestFailCounter, atLeast(ThresholdConfig.DEFAULT_RETRY_COUNT)).increment();
        }
    }

    @Test
    void check_successful_event_test() {
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        cloudWatchLogsService.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 4));

        verify(requestSuccessCounter, atLeast(4)).increment();
    }

    @Test
    void check_event_handles_successfully_released_test() {
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        final Collection<Record<Event>> sampleEvents = getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 2);
        final Collection<EventHandle> sampleEventHandles = sampleEvents.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        cloudWatchLogsService.output(sampleEvents);

        for (EventHandle sampleEventHandle: sampleEventHandles) {
            verify(sampleEventHandle).release(true);
        }
    }

    @Test
    void check_event_handles_failed_released_test() {
        setMockClientThrowCWLException();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        final Collection<Record<Event>> sampleEvents = getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE);
        final Collection<EventHandle> sampleEventHandles = sampleEvents.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        try {
            cloudWatchLogsService.output(sampleEvents);
        } catch (RetransmissionLimitException e) {
            for (EventHandle sampleEventHandle: sampleEventHandles) {
                verify(sampleEventHandle).release(false);
            }
        }
    }

    /**
     * Tests if our json string is equal to the default event size in bytes.
     * 14 accounts for the "message": byte size.
     */
    @Test
    void check_event_size_correct_test() {
        ArrayList<Record<Event>> sampleEvents = (ArrayList<Record<Event>>) getSampleRecordsLarge(ThresholdConfig.DEFAULT_BATCH_SIZE, ThresholdConfig.DEFAULT_EVENT_SIZE * convertToBytesFromKiloBytes - messageKeyByteSize); //Accounts for the key string value.

        assertThat(sampleEvents.get(0).getData().toJsonString().length(), equalTo(ThresholdConfig.DEFAULT_EVENT_SIZE * convertToBytesFromKiloBytes));
    }

    @Test
    void check_max_size_threshold_fail_test() {
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        final Collection<Record<Event>> sampleEvents = getSampleRecordsLarge(ThresholdConfig.DEFAULT_BATCH_SIZE, ThresholdConfig.DEFAULT_EVENT_SIZE * convertToBytesFromKiloBytes - CloudWatchLogsService.LOG_EVENT_OVERHEAD_SIZE - messageKeyByteSize + 1);

        cloudWatchLogsService.output(sampleEvents);

        verify(successEventCounter, never()).increment(anyDouble());
        verify(requestSuccessCounter, never()).increment();
    }

    @Test
    void check_max_size_threshold_success_test() {
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        final Collection<Record<Event>> sampleEvents = getSampleRecordsLarge(ThresholdConfig.DEFAULT_BATCH_SIZE, (ThresholdConfig.DEFAULT_EVENT_SIZE * convertToBytesFromKiloBytes - messageKeyByteSize) - CloudWatchLogsService.LOG_EVENT_OVERHEAD_SIZE);

        cloudWatchLogsService.output(sampleEvents);

        verify(successEventCounter, atLeastOnce()).increment(anyDouble());
        verify(requestSuccessCounter, atLeastOnce()).increment();
    }

    @Test
    void check_max_request_size_threshold_fail_test() {
        setThresholdForTestingRequestSize(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST);
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        cloudWatchLogsService.output(getSampleRecordsLarge(1, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - messageKeyByteSize - CloudWatchLogsService.LOG_EVENT_OVERHEAD_SIZE + 1));

        verify(requestSuccessCounter, never()).increment();
    }

    @Test
    void check_max_request_size_threshold_success_test() {
        setThresholdForTestingRequestSize(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST);
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        cloudWatchLogsService.output(getSampleRecordsLarge(1, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - messageKeyByteSize - CloudWatchLogsService.LOG_EVENT_OVERHEAD_SIZE));

        verify(requestSuccessCounter, atLeast(1)).increment();
    }

    @Test
    void check_max_api_request_size_threshold_success_test() {
        setThresholdForTestingMaxRequestRequestSize();
        setMockClientNoErrors();
        CloudWatchLogsService cloudWatchLogsService = getCwlClientWithMemoryBuffer();

        cloudWatchLogsService.output(getSampleRecordsLarge(1, (ThresholdConfig.DEFAULT_SIZE_OF_REQUEST * 2) - messageKeyByteSize - CloudWatchLogsService.LOG_EVENT_OVERHEAD_SIZE));

        verify(requestSuccessCounter, atLeast(1)).increment();
    }
}