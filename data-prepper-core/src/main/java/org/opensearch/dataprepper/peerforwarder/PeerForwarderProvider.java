/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.Map;
import java.util.Set;

public interface PeerForwarderProvider {
    /**
     * Registers a pipeline and identification keys
     *
     * @param pipelineName pipeline name
     * @param processor processor
     * @param pluginId plugin id
     * @param identificationKeys identification keys
     * @param pipelineWorkerThreads number of pipeline worker threads
     * @return peer forwarder
     * @since 2.9
     */
    PeerForwarder register(final String pipelineName, final Processor processor, final String pluginId, final Set<String> identificationKeys, final Integer pipelineWorkerThreads);

    /**
     * Returns if peer forwarding required
     *
     * @return returns if peer forwarding required or nto
     * @since 2.9
     */
    boolean isPeerForwardingRequired();

    /**
     * Returns if peers configured
     *
     * @return returns if peers configured
     * @since 2.9
     */
    boolean arePeersConfigured();

    /**
     * Returns pipeline peer forwarder receive buffer map
     *
     * @return Map of buffer per pipeline per pluginId
     * @since 2.9
     */
    Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> getPipelinePeerForwarderReceiveBufferMap();
}

