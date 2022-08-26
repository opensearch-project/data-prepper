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
import org.opensearch.dataprepper.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PeerForwarderClient {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderClient.class);
    // TODO: Update the URI to something similar to "/event/forward"
    private static final String URI = "/log/ingest";
    private static final int ASYNC_REQUEST_THREAD_COUNT = 200;

    private final ObjectMapper objectMapper;
    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private ExecutorService executorService;
    private PeerClientPool peerClientPool;

    public PeerForwarderClient(final ObjectMapper objectMapper,
                               final PeerForwarderClientFactory peerForwarderClientFactory) {
        this.objectMapper = objectMapper;
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        executorService = Executors.newFixedThreadPool(ASYNC_REQUEST_THREAD_COUNT);
    }

    public AggregatedHttpResponse serializeRecordsAndSendHttpRequest(
            final Collection<Record<Event>> records,
            final String ipAddress,
            final String pluginId) {
        // TODO: decide the default values of peer forwarder configuration and move the PeerClientPool to constructor
        peerClientPool = peerForwarderClientFactory.setPeerClientPool();
        List<WireEvent> wireEventList = new ArrayList<>();

        for (Record<Event> record : records) {
            final Event event = record.getData();
            wireEventList.add(new WireEvent(event.getMetadata().getEventType(), event.toJsonString()));
        }

        final WireEvents wireEvents = new WireEvents(wireEventList, pluginId);

        String serializedJsonString;
        try {
            serializedJsonString = objectMapper.writeValueAsString(wireEvents);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        final WebClient client = peerClientPool.getClient(ipAddress);
        final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseCompletableFuture = processHttpRequest(client, serializedJsonString);

        AggregatedHttpResponse response = null;
        try {
            response = aggregatedHttpResponseCompletableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Problem with asynchronous peer forwarding", e);
        }

        return response;
    }

    private CompletableFuture<AggregatedHttpResponse> processHttpRequest(final WebClient client, final String content) {
        final String authority = client.uri().getAuthority();
        return CompletableFuture.supplyAsync(() ->
        {
            try {
                final CompletableFuture<AggregatedHttpResponse> aggregate = client.post(URI, content).aggregate();
                return aggregate.join();
            } catch (Exception e) {
                LOG.error("Failed to forward request to address: {}", authority, e);
                return null;
            }
        }, executorService);
    }

}