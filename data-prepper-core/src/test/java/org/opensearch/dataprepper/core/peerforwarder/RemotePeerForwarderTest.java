/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.opensearch.dataprepper.core.peerforwarder.HashRing;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.RECORDS_ACTUALLY_PROCESSED_LOCALLY;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.RECORDS_FAILED_FORWARDING;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.RECORDS_SUCCESSFULLY_FORWARDED;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.RECORDS_TO_BE_FORWARDED;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.RECORDS_TO_BE_PROCESSED_LOCALLY;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.RECORDS_MISSING_IDENTIFICATION_KEYS;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.REQUESTS_FAILED;
import static org.opensearch.dataprepper.core.peerforwarder.RemotePeerForwarder.REQUESTS_SUCCESSFUL;
import org.apache.commons.lang3.RandomStringUtils;

@ExtendWith(MockitoExtension.class)
class RemotePeerForwarderTest {
    private static final int TEST_BUFFER_CAPACITY = 20;
    private static final int TEST_BATCH_SIZE = 20;
    private static final int TEST_BATCH_DELAY = 800;
    private static final int TEST_LOCAL_WRITE_TIMEOUT = 400;
    private static final int TEST_TIMEOUT_IN_MILLIS = 400;
    private static final int FORWARDING_BATCH_SIZE = 5;
    private static final int FORWARDING_BATCH_QUEUE_DEPTH = 1;
    private static final Duration FORWARDING_BATCH_TIMEOUT = Duration.of(800, ChronoUnit.MILLIS);
    private static final int PIPELINE_WORKER_THREADS = 3;
    private static final String PIPELINE_NAME = UUID.randomUUID().toString();
    private static final String PLUGIN_ID = UUID.randomUUID().toString();

    @Mock
    private PeerForwarderClient peerForwarderClient;

    @Mock
    private HashRing hashRing;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter recordsToBeProcessedLocallyCounter;

    @Mock
    private Counter recordsActuallyProcessedLocallyCounter;

    @Mock
    private Counter recordsToBeForwardedCounter;

    @Mock
    private Counter recordsSuccessfullyForwardedCounter;

    @Mock
    private Counter recordsFailedForwardingCounter;

    @Mock
    private Counter recordsMissingIdentificationKeys;

    @Mock
    private Counter requestsFailedCounter;

    @Mock
    private Counter requestsSuccessfulCounter;

    private String pipelineName;
    private String pluginId;
    private Set<String> identificationKeys;
    private PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer;

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
        identificationKeys = generateIdentificationKeys();
        peerForwarderReceiveBuffer = new PeerForwarderReceiveBuffer<>(TEST_BUFFER_CAPACITY, TEST_BATCH_SIZE, PIPELINE_NAME, PLUGIN_ID);

        when(pluginMetrics.counter(RECORDS_TO_BE_PROCESSED_LOCALLY)).thenReturn(recordsToBeProcessedLocallyCounter);
        when(pluginMetrics.counter(RECORDS_ACTUALLY_PROCESSED_LOCALLY)).thenReturn(recordsActuallyProcessedLocallyCounter);
        when(pluginMetrics.counter(RECORDS_TO_BE_FORWARDED)).thenReturn(recordsToBeForwardedCounter);
        when(pluginMetrics.counter(RECORDS_SUCCESSFULLY_FORWARDED)).thenReturn(recordsSuccessfullyForwardedCounter);
        when(pluginMetrics.counter(RECORDS_FAILED_FORWARDING)).thenReturn(recordsFailedForwardingCounter);
        when(pluginMetrics.counter(RECORDS_MISSING_IDENTIFICATION_KEYS)).thenReturn(recordsMissingIdentificationKeys);
        when(pluginMetrics.counter(REQUESTS_FAILED)).thenReturn(requestsFailedCounter);
        when(pluginMetrics.counter(REQUESTS_SUCCESSFUL)).thenReturn(requestsSuccessfulCounter);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(
                recordsToBeProcessedLocallyCounter,
                recordsActuallyProcessedLocallyCounter,
                recordsToBeForwardedCounter,
                recordsSuccessfullyForwardedCounter,
                recordsFailedForwardingCounter,
                requestsFailedCounter,
                requestsSuccessfulCounter
        );
    }

    private RemotePeerForwarder createObjectUnderTest() {
        return new RemotePeerForwarder(peerForwarderClient, hashRing, peerForwarderReceiveBuffer, pipelineName, pluginId, identificationKeys, pluginMetrics,
                TEST_BATCH_DELAY, TEST_LOCAL_WRITE_TIMEOUT, FORWARDING_BATCH_SIZE, FORWARDING_BATCH_QUEUE_DEPTH, FORWARDING_BATCH_TIMEOUT, PIPELINE_WORKER_THREADS);
    }

    @Test
    void test_forwardRecords_with_two_local_ips_should_process_record_two_record_locally() {
        final List<String> testIps = List.of("127.0.0.1", "128.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final Collection<Record<Event>> testRecords = generateBatchRecords(2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verifyNoInteractions(peerForwarderClient);
        assertThat(records.size(), equalTo(2));
        assertThat(records, equalTo(testRecords));

        verify(recordsToBeProcessedLocallyCounter).increment(2.0);
        verify(recordsActuallyProcessedLocallyCounter).increment(2.0);
    }

    @Test
    void test_forwardRecords_with_one_local_ip_and_one_remote_ip_should_process_record_one_record_locally() {
        mockPeerForwarderClientResponse();

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final int recordsSetsToGenerate = FORWARDING_BATCH_SIZE;
        final Collection<Record<Event>> testRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records.size(), equalTo(recordsSetsToGenerate));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(recordsSetsToGenerate);
    }

    @Test
    void forwardRecords_should_process_all_input_events_locally_when_client_throws() {
        when(peerForwarderClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString())).thenThrow(RuntimeException.class);

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final int recordsSetsToGenerate = FORWARDING_BATCH_SIZE;
        final Collection<Record<Event>> inputRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(inputRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(recordsSetsToGenerate));

        final Collection<Record<Event>> receivedRecords = peerForwarder.receiveRecords();
        assertThat(receivedRecords, notNullValue());
        assertThat(receivedRecords.size(), equalTo(recordsSetsToGenerate));

        final Collection<Record<Event>> totalRecords = records;
        totalRecords.addAll(receivedRecords);
        for (Record<Event> inputRecord : inputRecords) {
            assertThat(totalRecords, hasItem(inputRecord));
        }

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter, times(2)).increment(recordsSetsToGenerate);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(recordsFailedForwardingCounter).increment(recordsSetsToGenerate);
        verify(requestsFailedCounter).increment();
    }

    @Test
    void forwardRecords_should_flush_partial_batch_after_timeout() throws InterruptedException {
        mockPeerForwarderClientResponse();

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final int recordsSetsToGenerate = new Random().nextInt(FORWARDING_BATCH_SIZE - 1) + 1;
        final Collection<Record<Event>> inputRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        // Send first forwarding request with record count under the batch size
        final Collection<Record<Event>> firstRoundRecords = peerForwarder.forwardRecords(inputRecords);
        verifyNoInteractions(peerForwarderClient);

        assertThat(firstRoundRecords, notNullValue());
        assertThat(firstRoundRecords.size(), equalTo(recordsSetsToGenerate));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verifyNoInteractions(requestsSuccessfulCounter);
        verifyNoInteractions(recordsSuccessfullyForwardedCounter);

        // Wait longer than the batch timeout then send second forwarding request with no new records
        Thread.sleep(FORWARDING_BATCH_TIMEOUT.toMillis() + 1000);
        final Collection<Record<Event>> secondRoundRecords = peerForwarder.forwardRecords(Collections.emptyList());
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());

        assertThat(secondRoundRecords, notNullValue());
        assertThat(secondRoundRecords.size(), equalTo(0));

        verify(recordsActuallyProcessedLocallyCounter).increment(0);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(recordsSetsToGenerate);
    }

    @Test
    void forwardRecords_should_only_flush_batch_size() {
        mockPeerForwarderClientResponse();

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final int recordsSetsToGenerate = new Random().nextInt(FORWARDING_BATCH_SIZE - 1) + FORWARDING_BATCH_SIZE;
        final Collection<Record<Event>> inputRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(inputRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());

        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(recordsSetsToGenerate));
        assertThat(peerForwarder.peerBatchingQueueMap.get(testIps.get(0)).size(), equalTo(recordsSetsToGenerate - FORWARDING_BATCH_SIZE));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(FORWARDING_BATCH_SIZE);
    }

    @Test
    void forwardRecords_should_flush_all_batches() {
        mockPeerForwarderClientResponse();

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final int batches = PIPELINE_WORKER_THREADS;
        final int recordsSetsToGenerate = batches * FORWARDING_BATCH_SIZE;
        final Collection<Record<Event>> inputRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(inputRecords);
        verify(peerForwarderClient, times(batches)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());

        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(recordsSetsToGenerate));
        assertThat(peerForwarder.peerBatchingQueueMap.get(testIps.get(0)).isEmpty(), equalTo(true));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(requestsSuccessfulCounter, times(batches)).increment();
        verify(recordsSuccessfullyForwardedCounter, times(batches)).increment(FORWARDING_BATCH_SIZE);
    }

    @Test
    void forwardRecords_should_process_data_locally_if_forwarding_buffer_full() {
        mockPeerForwarderClientResponse();

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final int batches = PIPELINE_WORKER_THREADS;
        final int recordsSetsToGenerate = FORWARDING_BATCH_SIZE * batches + 1;
        final Collection<Record<Event>> inputRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(inputRecords);
        verify(peerForwarderClient, times(batches)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());

        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(recordsSetsToGenerate + 1));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate + 1);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(recordsFailedForwardingCounter).increment(1);
        verify(requestsSuccessfulCounter, times(batches)).increment();
        verify(recordsSuccessfullyForwardedCounter, times(batches)).increment(FORWARDING_BATCH_SIZE);
    }

    @Test
    void forwardRecords_should_process_data_locally_if_forwarding_future_is_exceptional() {
        final AggregatedHttpResponse aggregatedHttpResponse = mock(AggregatedHttpResponse.class);
        when(aggregatedHttpResponse.status()).thenReturn(HttpStatus.OK);
        when(peerForwarderClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
                .thenReturn(CompletableFuture.completedFuture(aggregatedHttpResponse));

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final int batches = 2;
        final int recordsSetsToGenerate = FORWARDING_BATCH_SIZE * batches;
        final Collection<Record<Event>> inputRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(inputRecords);
        verify(peerForwarderClient, times(batches)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());

        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(recordsSetsToGenerate));

        final Collection<Record<Event>> receivedRecords = peerForwarder.receiveRecords();
        assertThat(receivedRecords, notNullValue());
        assertThat(receivedRecords.size(), equalTo(FORWARDING_BATCH_SIZE));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate);
        verify(recordsActuallyProcessedLocallyCounter).increment(FORWARDING_BATCH_SIZE);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(requestsFailedCounter).increment();
        verify(recordsFailedForwardingCounter).increment(FORWARDING_BATCH_SIZE);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(FORWARDING_BATCH_SIZE);
    }

    @Test
    void test_receiveRecords_should_return_record_from_buffer() throws Exception {
        final Collection<Record<Event>> testRecords = generateBatchRecords(3);
        peerForwarderReceiveBuffer.writeAll(testRecords, TEST_TIMEOUT_IN_MILLIS);

        final RemotePeerForwarder objectUnderTest = createObjectUnderTest();
        final Collection<Record<Event>> records = objectUnderTest.receiveRecords();

        assertThat(records.size(), equalTo(testRecords.size()));
        assertThat(records, equalTo(testRecords));
    }

    @Test
    void test_receiveRecords_with_missing_identification_keys() {
        mockPeerForwarderClientResponse();

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final int recordsSetsToGenerate = new Random().nextInt(FORWARDING_BATCH_SIZE) + FORWARDING_BATCH_SIZE;
        final Collection<Record<Event>> testRecords = generateSetsofBatchRecords(recordsSetsToGenerate, 2);
        // Add an event that doesn't have identification keys in it
        final Map<String, String> eventData = new HashMap<>();
        eventData.put("key1", "8.8.8.8");
        eventData.put(RandomStringUtils.randomAlphabetic(5), RandomStringUtils.randomAlphabetic(10));
        final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
        testRecords.add(new Record<>(event));

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records.size(), equalTo(recordsSetsToGenerate + 1));

        verify(recordsToBeProcessedLocallyCounter).increment(recordsSetsToGenerate + 1);
        verify(recordsActuallyProcessedLocallyCounter).increment(recordsSetsToGenerate + 1);
        verify(recordsMissingIdentificationKeys, times(0)).increment(1.0);
        verify(recordsToBeForwardedCounter).increment(recordsSetsToGenerate);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(FORWARDING_BATCH_SIZE);
    }

    @Test
    void test_receiveRecords_with_no_identification_keys() {
        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final Collection<Record<Event>> testRecords = new ArrayList<>();
        // Add an event that doesn't have identification keys in it
        for (int i = 0; i < 2; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put(RandomStringUtils.randomAlphabetic(5), RandomStringUtils.randomAlphabetic(10));
            eventData.put(RandomStringUtils.randomAlphabetic(5), RandomStringUtils.randomAlphabetic(10));
            final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
            testRecords.add(new Record<>(event));
        }

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verifyNoInteractions(peerForwarderClient);
        assertThat(records.size(), equalTo(2));

        verify(recordsToBeProcessedLocallyCounter).increment(2.0);
        verify(recordsActuallyProcessedLocallyCounter).increment(2.0);
        verify(recordsMissingIdentificationKeys, times(2)).increment(1.0);
        verify(recordsToBeForwardedCounter, times(0)).increment(0.0);
    }

    @Test
    void test_processFailedRequestsLocally_null_http_response_populates_in_local_buffer() {
        final RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final int recordCount = new Random().nextInt(10) + 1;
        final Collection<Record<Event>> inputRecords = generateBatchRecords(recordCount);

        peerForwarder.processFailedRequestsLocally(null, inputRecords);
        final Collection<Record<Event>> receivedRecords = peerForwarder.receiveRecords();

        validateFailedForwardingRecords(receivedRecords, inputRecords, recordCount);
        validateFailedForwardingMetrics(recordCount);
    }

    @Test
    void test_processFailedRequestsLocally_bad_http_response_populates_in_local_buffer() {
        final RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final int recordCount = new Random().nextInt(10) + 1;
        final Collection<Record<Event>> inputRecords = generateBatchRecords(recordCount);

        peerForwarder.processFailedRequestsLocally(AggregatedHttpResponse.of(HttpStatus.BAD_REQUEST), inputRecords);
        final Collection<Record<Event>> receivedRecords = peerForwarder.receiveRecords();

        validateFailedForwardingRecords(receivedRecords, inputRecords, recordCount);
        validateFailedForwardingMetrics(recordCount);
    }

    @Test
    void test_processFailedRequestsLocally_exception_writing_to_local_buffer() {
        final RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final int recordCount = 100;
        final Collection<Record<Event>> inputRecords = generateBatchRecords(recordCount);

        peerForwarder.processFailedRequestsLocally(AggregatedHttpResponse.of(HttpStatus.BAD_REQUEST), inputRecords);
        final Collection<Record<Event>> receivedRecords = peerForwarder.receiveRecords();

        validateFailedForwardingRecords(receivedRecords, inputRecords, 0);
        verify(recordsFailedForwardingCounter).increment(recordCount);
        verify(requestsFailedCounter).increment();
    }

    @Test
    void test_processFailedRequestsLocally_no_failure() {
        final RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final int recordCount = new Random().nextInt(10) + 1;
        final Collection<Record<Event>> inputRecords = generateBatchRecords(recordCount);

        peerForwarder.processFailedRequestsLocally(AggregatedHttpResponse.of(HttpStatus.OK), inputRecords);
        final Collection<Record<Event>> receivedRecords = peerForwarder.receiveRecords();

        validateFailedForwardingRecords(receivedRecords, inputRecords, 0);
        verify(recordsSuccessfullyForwardedCounter).increment(recordCount);
        verify(requestsSuccessfulCounter).increment();
    }

    private void validateFailedForwardingRecords(final Collection<Record<Event>> receivedRecords, final Collection<Record<Event>> inputRecords,
                                                 final int expectedRecordCount) {
        assertThat(receivedRecords, notNullValue());
        assertThat(receivedRecords.size(), equalTo(expectedRecordCount));
        for (Record<Event> recivedRecord : receivedRecords) {
            assertThat(inputRecords, hasItem(recivedRecord));
        }
    }

    private void validateFailedForwardingMetrics(final int expectedRecordCount) {
        verify(recordsActuallyProcessedLocallyCounter).increment(expectedRecordCount);
        verify(recordsFailedForwardingCounter).increment(expectedRecordCount);
        verify(requestsFailedCounter).increment();
    }

    private Collection<Record<Event>> generateSetsofBatchRecords(final int recordsSetsToGenerate, final int numRecords) {
        return IntStream.range(0, recordsSetsToGenerate)
                .mapToObj(i -> generateBatchRecords(numRecords))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    private Collection<Record<Event>> generateBatchRecords(final int numRecords) {
        final Collection<Record<Event>> results = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value" + i);
            eventData.put("key2", "value" + i);
            final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
            results.add(new Record<>(event));
        }
        return results;
    }

    private Set<String> generateIdentificationKeys() {
        return Set.of("key1", "key2");
    }

    private void mockPeerForwarderClientResponse() {
        final Iterator<AggregatedHttpResponse> aggregatedHttpResponses = IntStream.range(0, PIPELINE_WORKER_THREADS)
                        .mapToObj(i -> {
                            final AggregatedHttpResponse aggregatedHttpResponse = mock(AggregatedHttpResponse.class);
                            when(aggregatedHttpResponse.status()).thenReturn(HttpStatus.OK);
                            return aggregatedHttpResponse;
                        }).iterator();

        when(peerForwarderClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString()))
                .thenAnswer(i -> CompletableFuture.completedFuture(aggregatedHttpResponses.next()));

    }
}
