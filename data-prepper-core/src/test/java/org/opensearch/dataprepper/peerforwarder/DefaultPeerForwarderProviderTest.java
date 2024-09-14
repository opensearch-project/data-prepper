/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultPeerForwarderProviderTest {
    private static final int PIPELINE_WORKER_THREADS = new Random().nextInt(10) + 1;

    @Mock
    private PeerForwarderClientFactory peerForwarderClientFactory;

    @Mock
    private PeerForwarderClient peerForwarderClient;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    private HashRing hashRing;

    @Mock
    private Processor processor;

    @Mock
    private PluginMetrics pluginMetrics;

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
        lenient().when(peerForwarderConfiguration.getFailedForwardingRequestLocalWriteTimeout()).thenReturn(500);
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.LOCAL_NODE);
    }

    private PeerForwarderProvider createObjectUnderTest() {
        return new DefaultPeerForwarderProvider(peerForwarderClientFactory, peerForwarderClient, peerForwarderConfiguration, pluginMetrics);
    }

    @Test
    void register_creates_a_new_RemotePeerForwarder_with_cloud_map_discovery_mode() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.AWS_CLOUD_MAP);
        final PeerForwarder peerForwarder = createObjectUnderTest().register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThat(peerForwarder, instanceOf(RemotePeerForwarder.class));
    }

    @Test
    void register_creates_a_new_RemotePeerForwarder_with_static_discovery_mode_of_size_grater_than_one() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.STATIC);
        when(peerForwarderConfiguration.getStaticEndpoints()).thenReturn(List.of("endpoint1", "endpoint2"));
        final PeerForwarder peerForwarder = createObjectUnderTest().register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThat(peerForwarder, instanceOf(RemotePeerForwarder.class));
    }

    @Test
    void register_creates_a_new_RemotePeerForwarder_with_static_discovery_mode_of_size_one() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.STATIC);
        when(peerForwarderConfiguration.getStaticEndpoints()).thenReturn(List.of("endpoint1"));
        final PeerForwarder peerForwarder = createObjectUnderTest().register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThat(peerForwarder, instanceOf(LocalPeerForwarder.class));
    }

    @Test
    void register_creates_a_new_LocalPeerForwarder_with_local_discovery_mode() {
        final PeerForwarder peerForwarder = createObjectUnderTest().register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThat(peerForwarder, instanceOf(LocalPeerForwarder.class));
    }

    @Test
    void register_creates_HashRing_if_peer_forwarding_is_required() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.AWS_CLOUD_MAP);
        createObjectUnderTest().register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        verify(peerForwarderClientFactory).createHashRing();
    }

    @Test
    void register_called_multiple_times_creates_only_one_HashRing_if_peer_forwarding_is_required() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.AWS_CLOUD_MAP);
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        for (int i = 0; i < 10; i++)
            objectUnderTest.register(pipelineName, processor, UUID.randomUUID().toString(), identificationKeys, PIPELINE_WORKER_THREADS);

        verify(peerForwarderClientFactory, times(1)).createHashRing();
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_return_false_if_register_is_not_called() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isPeerForwardingRequired(), equalTo(false));
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_throw_when_register_is_called_with_same_pipeline_and_plugin() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThrows(RuntimeException.class, () ->
                objectUnderTest.register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS));
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_return_false_if_register_is_called_with_local_discovery_mode() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThat(objectUnderTest.isPeerForwardingRequired(), equalTo(false));
    }

    @Test
    void isAtLeastOnePeerForwarderRegistered_should_return_true_if_register_is_called_with_cloud_map_discovery_mode() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.AWS_CLOUD_MAP);
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);

        assertThat(objectUnderTest.isPeerForwardingRequired(), equalTo(true));
    }

    @Test
    void getPipelinePeerForwarderReceiveBufferMap_should_return_empty_map_when_register_is_not_called() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();


        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = objectUnderTest
                .getPipelinePeerForwarderReceiveBufferMap();

        assertThat(objectUnderTest.isPeerForwardingRequired(), equalTo(false));
        assertThat(pipelinePeerForwarderReceiveBufferMap, is(notNullValue()));
        assertThat(pipelinePeerForwarderReceiveBufferMap.size(), equalTo(0));
    }

    @Test
    void getPipelinePeerForwarderReceiveBufferMap_should_return_non_empty_map_when_register_is_called() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        objectUnderTest.register(pipelineName, processor, UUID.randomUUID().toString(), identificationKeys, PIPELINE_WORKER_THREADS);

        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = objectUnderTest
                .getPipelinePeerForwarderReceiveBufferMap();

        assertThat(objectUnderTest.isPeerForwardingRequired(), equalTo(false));
        assertThat(pipelinePeerForwarderReceiveBufferMap, is(notNullValue()));
        assertThat(pipelinePeerForwarderReceiveBufferMap.size(), equalTo(1));
        assertThat(pipelinePeerForwarderReceiveBufferMap.containsKey(pipelineName), equalTo(true));
    }
}
