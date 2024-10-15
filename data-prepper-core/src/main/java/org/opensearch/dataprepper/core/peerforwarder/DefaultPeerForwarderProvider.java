/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.core.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultPeerForwarderProvider implements PeerForwarderProvider {

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final PeerForwarderClient peerForwarderClient;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PluginMetrics pluginMetrics;
    private final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
    private HashRing hashRing;

    DefaultPeerForwarderProvider(final PeerForwarderClientFactory peerForwarderClientFactory,
                          final PeerForwarderClient peerForwarderClient,
                          final PeerForwarderConfiguration peerForwarderConfiguration,
                          final PluginMetrics pluginMetrics) {
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.peerForwarderClient = peerForwarderClient;
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.pluginMetrics = pluginMetrics;
    }

    public PeerForwarder register(final String pipelineName, final Processor processor, final String pluginId, final Set<String> identificationKeys,
                                  final Integer pipelineWorkerThreads) {
        if (pipelinePeerForwarderReceiveBufferMap.containsKey(pipelineName) &&
                pipelinePeerForwarderReceiveBufferMap.get(pipelineName).containsKey(pluginId)) {
            throw new RuntimeException("Data Prepper 2.0 will only support a single peer-forwarder per pipeline/plugin type");
        }

        final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer = createBufferPerPipelineProcessor(pipelineName, pluginId);

        if (isPeerForwardingRequired()) {
            if (hashRing == null) {
                hashRing = peerForwarderClientFactory.createHashRing();
            }
            return new RemotePeerForwarder(
                    peerForwarderClient,
                    hashRing,
                    peerForwarderReceiveBuffer,
                    pipelineName,
                    pluginId,
                    identificationKeys,
                    pluginMetrics,
                    peerForwarderConfiguration.getBatchDelay(),
                    peerForwarderConfiguration.getFailedForwardingRequestLocalWriteTimeout(),
                    peerForwarderConfiguration.getForwardingBatchSize(),
                    peerForwarderConfiguration.getForwardingBatchQueueDepth(),
                    peerForwarderConfiguration.getForwardingBatchTimeout(),
                    pipelineWorkerThreads
            );
        }
        else {
            return new LocalPeerForwarder();
        }
    }

    private PeerForwarderReceiveBuffer<Record<Event>> createBufferPerPipelineProcessor(final String pipelineName, final String pluginId) {
        final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer = new
                PeerForwarderReceiveBuffer<>(peerForwarderConfiguration.getBufferSize(), peerForwarderConfiguration.getBatchSize(), pipelineName, pluginId);

        final Map<String, PeerForwarderReceiveBuffer<Record<Event>>> pluginsBufferMap =
                pipelinePeerForwarderReceiveBufferMap.computeIfAbsent(pipelineName, k -> new HashMap<>());

        pluginsBufferMap.put(pluginId, peerForwarderReceiveBuffer);

        return peerForwarderReceiveBuffer;
    }

    public boolean isPeerForwardingRequired() {
        return arePeersConfigured() && pipelinePeerForwarderReceiveBufferMap.size() > 0;
    }

    public boolean arePeersConfigured() {
        final DiscoveryMode discoveryMode = peerForwarderConfiguration.getDiscoveryMode();
        if (discoveryMode.equals(DiscoveryMode.LOCAL_NODE)) {
            return false;
        }
        else if (discoveryMode.equals(DiscoveryMode.STATIC) && peerForwarderConfiguration.getStaticEndpoints().size() <= 1) {
            return false;
        }
        return true;
    }

    public Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> getPipelinePeerForwarderReceiveBufferMap() {
        return pipelinePeerForwarderReceiveBufferMap;
    }
}
