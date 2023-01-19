/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_ACTUALLY_PROCESSED_LOCALLY;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_FAILED_FORWARDING;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_RECEIVED_FROM_PEERS;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_SUCCESSFULLY_FORWARDED;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_TO_BE_FORWARDED;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_TO_BE_PROCESSED_LOCALLY;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.RECORDS_MISSING_IDENTIFICATION_KEYS;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.REQUESTS_FAILED;
import static org.opensearch.dataprepper.peerforwarder.RemotePeerForwarder.REQUESTS_SUCCESSFUL;
import org.apache.commons.lang3.RandomStringUtils;

@ExtendWith(MockitoExtension.class)
class RemotePeerForwarderTest {
    private static final int TEST_BUFFER_CAPACITY = 3;
    private static final int TEST_BATCH_SIZE = 3;
    private static final int TEST_BATCH_DELAY = 3_000;
    private static final int TEST_TIMEOUT_IN_MILLIS = 500;

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
    private Counter recordsReceivedFromPeersCounter;

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
        peerForwarderReceiveBuffer = new PeerForwarderReceiveBuffer<>(TEST_BUFFER_CAPACITY, TEST_BATCH_SIZE);

        when(pluginMetrics.counter(RECORDS_TO_BE_PROCESSED_LOCALLY)).thenReturn(recordsToBeProcessedLocallyCounter);
        when(pluginMetrics.counter(RECORDS_ACTUALLY_PROCESSED_LOCALLY)).thenReturn(recordsActuallyProcessedLocallyCounter);
        when(pluginMetrics.counter(RECORDS_TO_BE_FORWARDED)).thenReturn(recordsToBeForwardedCounter);
        when(pluginMetrics.counter(RECORDS_SUCCESSFULLY_FORWARDED)).thenReturn(recordsSuccessfullyForwardedCounter);
        when(pluginMetrics.counter(RECORDS_FAILED_FORWARDING)).thenReturn(recordsFailedForwardingCounter);
        when(pluginMetrics.counter(RECORDS_RECEIVED_FROM_PEERS)).thenReturn(recordsReceivedFromPeersCounter);
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
                recordsReceivedFromPeersCounter,
                requestsFailedCounter,
                requestsSuccessfulCounter
        );
    }

    private RemotePeerForwarder createObjectUnderTest() {
        return new RemotePeerForwarder(peerForwarderClient, hashRing, peerForwarderReceiveBuffer, pipelineName, pluginId, identificationKeys, pluginMetrics, TEST_BATCH_DELAY);
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
        AggregatedHttpResponse aggregatedHttpResponse = mock(AggregatedHttpResponse.class);
        when(aggregatedHttpResponse.status()).thenReturn(HttpStatus.OK);
        when(peerForwarderClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString())).thenReturn(aggregatedHttpResponse);

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final Collection<Record<Event>> testRecords = generateBatchRecords(2);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records.size(), equalTo(1));

        verify(recordsToBeProcessedLocallyCounter).increment(1.0);
        verify(recordsActuallyProcessedLocallyCounter).increment(1.0);
        verify(recordsToBeForwardedCounter).increment(1.0);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(1.0);
    }

    @Test
    void forwardRecords_should_return_all_input_events_when_client_throws() {
        when(peerForwarderClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString())).thenThrow(RuntimeException.class);

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        final RemotePeerForwarder peerForwarder = createObjectUnderTest();

        final Collection<Record<Event>> inputRecords = generateBatchRecords(2);
        final Collection<Record<Event>> records = peerForwarder.forwardRecords(inputRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(inputRecords.size()));
        for (Record<Event> inputRecord : inputRecords) {
            assertThat(records, hasItem(inputRecord));
        }

        verify(recordsToBeProcessedLocallyCounter).increment(1.0);
        verify(recordsActuallyProcessedLocallyCounter).increment(2.0);
        verify(recordsToBeForwardedCounter).increment(1.0);
        verify(recordsFailedForwardingCounter).increment(1.0);
        verify(requestsFailedCounter).increment();
    }

    @Test
    void test_receiveRecords_should_return_record_from_buffer() throws Exception {
        final Collection<Record<Event>> testRecords = generateBatchRecords(3);
        peerForwarderReceiveBuffer.writeAll(testRecords, TEST_TIMEOUT_IN_MILLIS);

        final RemotePeerForwarder objectUnderTest = createObjectUnderTest();
        final Collection<Record<Event>> records = objectUnderTest.receiveRecords();

        assertThat(records.size(), equalTo(testRecords.size()));
        assertThat(records, equalTo(testRecords));

        verify(recordsReceivedFromPeersCounter).increment(3.0);
    }

    @Test
    void test_receiveRecords_with_missing_identification_keys() throws Exception {
        AggregatedHttpResponse aggregatedHttpResponse = mock(AggregatedHttpResponse.class);
        when(aggregatedHttpResponse.status()).thenReturn(HttpStatus.OK);
        when(peerForwarderClient.serializeRecordsAndSendHttpRequest(anyCollection(), anyString(), anyString(), anyString())).thenReturn(aggregatedHttpResponse);

        final List<String> testIps = List.of("8.8.8.8", "127.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final Collection<Record<Event>> testRecords = generateBatchRecords(2);
        // Add an event that doesn't have identification keys in it
        final Map<String, String> eventData = new HashMap<>();
        eventData.put("key1", "8.8.8.8");
        eventData.put(RandomStringUtils.randomAlphabetic(5), RandomStringUtils.randomAlphabetic(10));
        final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
        testRecords.add(new Record<>(event));

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records.size(), equalTo(2));

        verify(recordsToBeProcessedLocallyCounter).increment(2.0);
        verify(recordsActuallyProcessedLocallyCounter).increment(2.0);
        verify(recordsMissingIdentificationKeys, times(0)).increment(1.0);
        verify(recordsToBeForwardedCounter).increment(1.0);
        verify(requestsSuccessfulCounter).increment();
        verify(recordsSuccessfullyForwardedCounter).increment(1.0);
    }

    @Test
    void test_receiveRecords_with_no_identification_keys() throws Exception {
        AggregatedHttpResponse aggregatedHttpResponse = mock(AggregatedHttpResponse.class);

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
        verify(peerForwarderClient, times(0)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records.size(), equalTo(2));

        verify(recordsToBeProcessedLocallyCounter).increment(2.0);
        verify(recordsActuallyProcessedLocallyCounter).increment(2.0);
        verify(recordsMissingIdentificationKeys, times(2)).increment(1.0);
        verify(recordsToBeForwardedCounter, times(0)).increment(0.0);
        verify(requestsSuccessfulCounter, times(0)).increment();
        verify(recordsSuccessfullyForwardedCounter, times(0)).increment(0.0);
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

}
