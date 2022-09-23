/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.UnprocessedRequestException;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;

public class PeerForwarderClient {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderClient.class);
    public static final String REQUESTS = "requests";
    public static final String LATENCY = "request_latency";
    public static final String ERRORS = "failed_requests";
    public static final String DESTINATION = "destination";

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final ObjectMapper objectMapper;
    private ExecutorService executorService;
    private final PluginMetrics pluginMetrics;
    private PeerClientPool peerClientPool;

    private final Map<String, Timer> forwardRequestTimers;
    private final Map<String, Counter> forwardedRequestCounters;

    public PeerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                               final PeerForwarderClientFactory peerForwarderClientFactory,
                               final ObjectMapper objectMapper,
                               final PluginMetrics pluginMetrics) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.objectMapper = objectMapper;
        this.pluginMetrics = pluginMetrics;
        executorService = Executors.newFixedThreadPool(peerForwarderConfiguration.getClientThreadCount());
        forwardedRequestCounters = new ConcurrentHashMap<>();

        forwardRequestTimers = new ConcurrentHashMap<>();
    }

    public AggregatedHttpResponse serializeRecordsAndSendHttpRequest(
            final Collection<Record<Event>> records,
            final String ipAddress,
            final String pluginId,
            final String pipelineName) {
        // TODO: decide the default values of peer forwarder configuration and move the PeerClientPool to constructor
        peerClientPool = peerForwarderClientFactory.setPeerClientPool();

        final WebClient client = peerClientPool.getClient(ipAddress);

        final String serializedJsonString = getSerializedJsonString(records, pluginId, pipelineName);

        final Timer forwardRequestTimer = forwardRequestTimers.computeIfAbsent(
                ipAddress, ip -> pluginMetrics.timerWithTags(LATENCY, DESTINATION, ip));
        final Counter forwardedRequestCounter = forwardedRequestCounters.computeIfAbsent(
                ipAddress, ip -> pluginMetrics.counterWithTags(REQUESTS, DESTINATION, ip));

        final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseCompletableFuture = forwardRequestTimer.record(() ->
            processHttpRequest(client, serializedJsonString)
        );
        forwardedRequestCounter.increment();

        return getAggregatedHttpResponse(aggregatedHttpResponseCompletableFuture);
    }

    private String getSerializedJsonString(final Collection<Record<Event>> records, final String pluginId, final String pipelineName) {
        final List<WireEvent> wireEventList = getWireEventList(records);
        final WireEvents wireEvents = new WireEvents(wireEventList, pluginId, pipelineName);

        try {
            return objectMapper.writeValueAsString(wireEvents);
        } catch (JsonProcessingException e) {
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

    private CompletableFuture<AggregatedHttpResponse> processHttpRequest(final WebClient client, final String content) {
        return CompletableFuture.supplyAsync(() ->
        {
            final CompletableFuture<AggregatedHttpResponse> aggregate = client.post(DEFAULT_PEER_FORWARDING_URI, content).aggregate();
            return aggregate.join();
        }, executorService);
    }

    private AggregatedHttpResponse getAggregatedHttpResponse(final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseCompletableFuture) throws UnprocessedRequestException {
        try {
            return aggregatedHttpResponseCompletableFuture.get();
        } catch (final InterruptedException e) {
            LOG.error("Peer forwarding interrupted.");
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException) e.getCause();
            throw new RuntimeException(e.getCause());
        }
    }

}