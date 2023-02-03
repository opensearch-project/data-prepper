/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderClientFactory;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;

public class PeerForwarderClient {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderClient.class);
    static final String REQUESTS = "requests";
    static final String CLIENT_REQUEST_FORWARDING_LATENCY = "clientRequestForwardingLatency";

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final Counter requestsCounter;
    private final Timer clientRequestForwardingLatencyTimer;

    private PeerClientPool peerClientPool;

    public PeerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                               final PeerForwarderClientFactory peerForwarderClientFactory,
                               final ObjectMapper objectMapper,
                               final PluginMetrics pluginMetrics) {
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.objectMapper = objectMapper;
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
        final List<WireEvent> wireEventList = getWireEventList(records);
        final WireEvents wireEvents = new WireEvents(wireEventList, pluginId, pipelineName);

        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(wireEvents);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<WireEvent> getWireEventList(final Collection<Record<Event>> records) {
        final List<WireEvent> wireEventList = new ArrayList<>();

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            wireEventList.add(getWireEvent(event));
        }
        return wireEventList;
    }

    private WireEvent getWireEvent(final Event event) {
        return new WireEvent(
                event.getMetadata().getEventType(),
                event.getMetadata().getTimeReceived(),
                event.getMetadata().getAttributes(),
                event.toJsonString()
        );
    }

    private CompletableFuture<AggregatedHttpResponse> processHttpRequest(final WebClient client, final byte[] content) {
        return CompletableFuture.supplyAsync(() ->
        {
            final CompletableFuture<AggregatedHttpResponse> aggregate = client.post(DEFAULT_PEER_FORWARDING_URI, content).aggregate();
            return aggregate.join();
        }, executorService);
    }
}