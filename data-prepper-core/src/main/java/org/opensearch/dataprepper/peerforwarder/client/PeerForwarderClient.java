/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.opensearch.dataprepper.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderClientFactory;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;

public class PeerForwarderClient {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderClient.class);

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final ObjectMapper objectMapper;
    private ExecutorService executorService;
    private PeerClientPool peerClientPool;

    public PeerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                               final PeerForwarderClientFactory peerForwarderClientFactory,
                               final ObjectMapper objectMapper) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.objectMapper = objectMapper;
        executorService = Executors.newFixedThreadPool(peerForwarderConfiguration.getClientThreadCount());
    }

    public AggregatedHttpResponse serializeRecordsAndSendHttpRequest(
            final Collection<Record<Event>> records,
            final String ipAddress,
            final String pluginId) {
        // TODO: decide the default values of peer forwarder configuration and move the PeerClientPool to constructor
        peerClientPool = peerForwarderClientFactory.setPeerClientPool();

        final WebClient client = peerClientPool.getClient(ipAddress);

        final String serializedJsonString = getSerializedJsonString(records, pluginId);
        if (serializedJsonString == null) {
            return AggregatedHttpResponse.of(HttpStatus.BAD_REQUEST);
        }

        final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseCompletableFuture = processHttpRequest(client, serializedJsonString);

        return getAggregatedHttpResponse(aggregatedHttpResponseCompletableFuture);
    }

    private String getSerializedJsonString(final Collection<Record<Event>> records, final String pluginId) {
        final List<WireEvent> wireEventList = getWireEventList(records);
        final WireEvents wireEvents = new WireEvents(wireEventList, pluginId);

        String serializedJsonString = null;
        try {
            serializedJsonString = objectMapper.writeValueAsString(wireEvents);
        } catch (JsonProcessingException e) {
            LOG.warn("Unable to send request to peer, processing locally.", e);
        }
        return serializedJsonString;
    }

    private List<WireEvent> getWireEventList(final Collection<Record<Event>> records) {
        List<WireEvent> wireEventList = new ArrayList<>();

        for (Record<Event> record : records) {
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

    private CompletableFuture<AggregatedHttpResponse> processHttpRequest(final WebClient client, final String content) {
        final String authority = client.uri().getAuthority();
        return CompletableFuture.supplyAsync(() ->
        {
            try {
                final CompletableFuture<AggregatedHttpResponse> aggregate = client.post(DEFAULT_PEER_FORWARDING_URI, content).aggregate();
                return aggregate.join();
            } catch (Exception e) {
                LOG.error("Failed to forward request to address: {}", authority, e);
                return null;
            }
        }, executorService);
    }

    private AggregatedHttpResponse getAggregatedHttpResponse(final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseCompletableFuture) {
        AggregatedHttpResponse response = null;
        try {
            response = aggregatedHttpResponseCompletableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Problem with asynchronous peer forwarding", e);
        }
        return response;
    }

}