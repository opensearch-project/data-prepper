/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;

import java.util.Map;
import java.util.Set;

public class LocalModePeerForwarderProvider implements PeerForwarderProvider {

    private final PeerForwarderProvider peerForwarderProvider;
    private boolean isRemotePeerForwarderRegistered;

    public LocalModePeerForwarderProvider(final PeerForwarderProvider peerForwarderProvider) {
        this.peerForwarderProvider = peerForwarderProvider;
        this.isRemotePeerForwarderRegistered = false;
    }

    @Override
    public PeerForwarder register(final String pipelineName, final Processor processor, final String pluginId, final Set<String> identificationKeys, final Integer pipelineWorkerThreads) {
        if (((RequiresPeerForwarding)processor).isForLocalProcessingOnly(null)) {
            return new LocalPeerForwarder();
        }
        isRemotePeerForwarderRegistered = true;
        return peerForwarderProvider.register(pipelineName, processor, pluginId, identificationKeys, pipelineWorkerThreads);
    }

    @Override
    public boolean isPeerForwardingRequired() {
        return isRemotePeerForwarderRegistered;
    }

    @Override
    public Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> getPipelinePeerForwarderReceiveBufferMap() {
        return (isRemotePeerForwarderRegistered) ?
            peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap() :
        Map.of();
    }

    @Override
    public boolean arePeersConfigured() {
        return isRemotePeerForwarderRegistered ? peerForwarderProvider.arePeersConfigured() : false;
    }
}
