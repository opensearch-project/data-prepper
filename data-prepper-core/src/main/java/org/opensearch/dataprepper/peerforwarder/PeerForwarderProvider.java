/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.record.Record;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PeerForwarderProvider {

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final PeerForwarderClient peerForwarderClient;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<?>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
    private HashRing hashRing;

    PeerForwarderProvider(final PeerForwarderClientFactory peerForwarderClientFactory,
                          final PeerForwarderClient peerForwarderClient,
                          final PeerForwarderConfiguration peerForwarderConfiguration) {
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.peerForwarderClient = peerForwarderClient;
        this.peerForwarderConfiguration = peerForwarderConfiguration;
    }

    public PeerForwarder register(final String pipelineName, final String pluginId, final Set<String> identificationKeys) {

        // TODO: Data Prepper 2.0 will only support a single peer-forwarder per pipeline/plugin type. Check this condition.

        if (pipelinePeerForwarderReceiveBufferMap.containsKey(pipelineName) &&
                pipelinePeerForwarderReceiveBufferMap.get(pipelineName).containsKey(pluginId)) {
            throw new RuntimeException("Data Prepper 2.0 will only support a single peer-forwarder per pipeline/plugin type");
        }

        if(hashRing == null) {
            hashRing = peerForwarderClientFactory.createHashRing();
        }

        createBufferPerPipelineProcessor(pipelineName, pluginId);

        if (isAtLeastOnePeerForwarderRegistered()) {
            return new RemotePeerForwarder(peerForwarderClient, hashRing, pipelineName, pluginId, identificationKeys);
        }
        else {
            return new LocalPeerForwarder();
        }
    }

    private void createBufferPerPipelineProcessor(final String pipelineName, final String pluginId) {
        if (pipelinePeerForwarderReceiveBufferMap.containsKey(pipelineName)) {
            pipelinePeerForwarderReceiveBufferMap.get(pipelineName).put(
                    pluginId,
                    new PeerForwarderReceiveBuffer<>(peerForwarderConfiguration.getBufferSize(), peerForwarderConfiguration.getBatchSize())
            );
        }
        else {
            Map<String, PeerForwarderReceiveBuffer<Record<?>>> peerForwarderReceiveBufferMap = new HashMap<>();
            peerForwarderReceiveBufferMap.put(
                    pluginId,
                    new PeerForwarderReceiveBuffer<>(peerForwarderConfiguration.getBufferSize(), peerForwarderConfiguration.getBatchSize())
            );
            pipelinePeerForwarderReceiveBufferMap.put(pipelineName, peerForwarderReceiveBufferMap);
        }
    }

    public boolean isAtLeastOnePeerForwarderRegistered() {
        return pipelinePeerForwarderReceiveBufferMap.size() > 0;
    }

    public Map<String, Map<String, PeerForwarderReceiveBuffer<Record<?>>>> getPipelinePeerForwarderReceiveBufferMap() {
        return pipelinePeerForwarderReceiveBufferMap;
    }
}
