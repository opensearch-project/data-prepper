/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.client;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.core.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderClientFactory;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.codec.PeerForwarderCodec;
import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class PeerForwarderClient {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderClient.class);
    static final String REQUESTS = "requests";
    static final String CLIENT_REQUEST_FORWARDING_LATENCY = "clientRequestForwardingLatency";

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerForwarderCodec peerForwarderCodec;
    private final ExecutorService executorService;
    private final Counter requestsCounter;
    private final Timer clientRequestForwardingLatencyTimer;

    private PeerClientPool peerClientPool;

    public PeerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                               final PeerForwarderClientFactory peerForwarderClientFactory,
                               final PeerForwarderCodec peerForwarderCodec,
                               final PluginMetrics pluginMetrics) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.peerForwarderCodec = peerForwarderCodec;
        executorService = Executors.newFixedThreadPool(peerForwarderConfiguration.getClientThreadCount());
        requestsCounter = pluginMetrics.counter(REQUESTS);
        clientRequestForwardingLatencyTimer = pluginMetrics.timer(CLIENT_REQUEST_FORWARDING_LATENCY);
    }

    public CompletableFuture<AggregatedHttpResponse> serializeRecordsAndSendHttpRequest(
            final Collection<Record<Event>> records,
            final String ipAddress,
            final String pluginId,
            final String pipelineName) {
        // TODO: Initialize this in the constructor in future.
        //  It doesn't work right now as default certificate and private key file paths are not valid while loading constructor.
        if (peerClientPool == null) {
            peerClientPool = peerForwarderClientFactory.setPeerClientPool();
        }

        final WebClient client = peerClientPool.getClient(ipAddress);

        final byte[] serializedJsonBytes = getSerializedJsonBytes(records, pluginId, pipelineName);

        final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponse = clientRequestForwardingLatencyTimer.record(() ->
            processHttpRequest(client, serializedJsonBytes)
        );
        requestsCounter.increment();

        return aggregatedHttpResponse;
    }

    private byte[] getSerializedJsonBytes(final Collection<Record<Event>> records, final String pluginId, final String pipelineName) {
        final List<Event> eventList = records.stream().map(Record::getData).collect(Collectors.toList());
        final PeerForwardingEvents peerForwardingEvents = new PeerForwardingEvents(eventList, pluginId, pipelineName);
        try {
            return peerForwarderCodec.serialize(peerForwardingEvents);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<AggregatedHttpResponse> processHttpRequest(final WebClient client, final byte[] content) {
        return CompletableFuture.supplyAsync(() ->
        {
            final CompletableFuture<AggregatedHttpResponse> aggregate = client.post(PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI, content).aggregate();
            return aggregate.join();
        }, executorService);
    }
}