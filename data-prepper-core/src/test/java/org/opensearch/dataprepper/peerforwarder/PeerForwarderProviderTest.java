/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PeerForwarderProviderTest {

    @Mock
    private PeerForwarderClientFactory peerForwarderClientFactory;

    @Mock
    private PeerForwarderClient peerForwarderClient;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    private HashRing hashRing;

    private String pipelineName;
    private String pluginId;
    private Set<String> identificationKeys;

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
        identificationKeys = Collections.singleton(UUID.randomUUID().toString());

        lenient().when(peerForwarderClientFactory.createHashRing()).thenReturn(hashRing);
        lenient().when(peerForwarderConfiguration.getBufferSize()).thenReturn(512);
        lenient().when(peerForwarderConfiguration.getBatchSize()).thenReturn(48);
    }

    private PeerForwarderProvider createObjectUnderTest() {
        return new PeerForwarderProvider(peerForwarderClientFactory, peerForwarderClient, peerForwarderConfiguration);
    }

    @Test
    void register_creates_a_new_RemotePeerForwarder() {
        final PeerForwarder peerForwarder = createObjectUnderTest().register(pipelineName, pluginId, identificationKeys);

        assertThat(peerForwarder, instanceOf(RemotePeerForwarder.class));
    }

    @Test
    void register_creates_HashRing() {
        createObjectUnderTest().register(pipelineName, pluginId, identificationKeys);

        verify(peerForwarderClientFactory).createHashRing();
    }

    @Test
    void register_called_multiple_times_creates_only_one_HashRing() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        for (int i = 0; i < 10; i++)
            objectUnderTest.register(pipelineName, UUID.randomUUID().toString(), identificationKeys);

        verify(peerForwarderClientFactory, times(1)).createHashRing();
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_return_false_if_register_is_not_called() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isAtLeastOnePeerForwarderRegistered(), equalTo(false));
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_throw_when_register_is_called_with_same_pipeline_and_plugin() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, pluginId, identificationKeys);

        assertThrows(RuntimeException.class, () ->
                objectUnderTest.register(pipelineName, pluginId, identificationKeys));
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_return_true_if_register_is_called_() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, pluginId, identificationKeys);

        assertThat(objectUnderTest.isAtLeastOnePeerForwarderRegistered(), equalTo(true));
    }

    @Test
    void getPipelinePeerForwarderReceiveBufferMap_should_return_empty_map_when_register_is_not_called() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();


        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<?>>>> pipelinePeerForwarderReceiveBufferMap = objectUnderTest
                .getPipelinePeerForwarderReceiveBufferMap();

        assertThat(objectUnderTest.isAtLeastOnePeerForwarderRegistered(), equalTo(false));
        assertThat(pipelinePeerForwarderReceiveBufferMap, is(notNullValue()));
        assertThat(pipelinePeerForwarderReceiveBufferMap.size(), equalTo(0));
    }

    @Test
    void getPipelinePeerForwarderReceiveBufferMap_should_return_non_empty_map_when_register_is_called() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, UUID.randomUUID().toString(), identificationKeys);

        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<?>>>> pipelinePeerForwarderReceiveBufferMap = objectUnderTest
                .getPipelinePeerForwarderReceiveBufferMap();

        assertThat(objectUnderTest.isAtLeastOnePeerForwarderRegistered(), equalTo(true));
        assertThat(pipelinePeerForwarderReceiveBufferMap, is(notNullValue()));
        assertThat(pipelinePeerForwarderReceiveBufferMap.size(), equalTo(1));
        assertThat(pipelinePeerForwarderReceiveBufferMap.containsKey(pipelineName), equalTo(true));
    }
}