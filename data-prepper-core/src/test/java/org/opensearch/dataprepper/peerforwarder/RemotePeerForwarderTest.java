/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.log.JacksonLog;
import com.amazon.dataprepper.model.record.Record;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RemotePeerForwarderTest {
    @Mock
    PeerForwarderClient peerForwarderClient;

    @Mock
    HashRing hashRing;
    private String pipelineName;
    private String pluginId;
    private Set<String> identificationKeys;

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
        identificationKeys = generateIdentificationKeys();
    }

    private RemotePeerForwarder createObjectUnderTest() {
        return new RemotePeerForwarder(peerForwarderClient, hashRing, pipelineName, pluginId, identificationKeys);
    }

    @Test
    void test_forwardRecords_with_two_local_ips_should_process_record_two_record_locally() {
        final List<String> testIps = List.of("127.0.0.1", "128.0.0.1");
        lenient().when(hashRing.getServerIp(List.of("value1", "value1"))).thenReturn(Optional.of(testIps.get(0)));
        lenient().when(hashRing.getServerIp(List.of("value2", "value2"))).thenReturn(Optional.of(testIps.get(1)));

        RemotePeerForwarder peerForwarder = createObjectUnderTest();
        final Collection<Record<Event>> testRecords = generateBatchRecords(2, false);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verifyNoInteractions(peerForwarderClient);
        assertThat(records.size(), equalTo(2));
        assertThat(records, equalTo(testRecords));
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
        final Collection<Record<Event>> testRecords = generateBatchRecords(2, false);

        final Collection<Record<Event>> records = peerForwarder.forwardRecords(testRecords);
        verify(peerForwarderClient, times(1)).serializeRecordsAndSendHttpRequest(anyList(), anyString(), anyString(), anyString());
        assertThat(records.size(), equalTo(1));
    }

    private Collection<Record<Event>> generateBatchRecords(final int numRecords, final boolean isSameValues) {
        final Collection<Record<Event>> results = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final Map<String, String> eventData = new HashMap<>();
            if (isSameValues) {
                eventData.put("key1", "value");
                eventData.put("key2", "value");
            }
            else {
                eventData.put("key1", "value" + i);
                eventData.put("key2", "value" + i);
            }
            final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
            results.add(new Record<>(event));
        }
        return results;
    }

    private Set<String> generateIdentificationKeys() {
        return Set.of("key1", "key2");
    }

}