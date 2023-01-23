/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import com.linecorp.armeria.common.HttpStatus;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderClientFactory;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;

public class PeerForwarderClient {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderClient.class);
    private static final int FAILED_REQUESTS_BUFFER_WRITE_TIMEOUT_MILLIS = 500;
    static final String REQUESTS = "requests";
    static final String RECORDS_SUCCESSFULLY_FORWARDED = "recordsSuccessfullyForwarded";
    static final String RECORDS_FAILED_FORWARDING = "recordsFailedForwarding";
    static final String REQUESTS_FAILED = "requestsFailed";
    static final String REQUESTS_SUCCESSFUL = "requestsSuccessful";
    static final String CLIENT_REQUEST_FORWARDING_LATENCY = "clientRequestForwardingLatency";

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final Counter requestsCounter;
    private final Counter recordsSuccessfullyForwardedCounter;
    private final Counter recordsFailedForwardingCounter;
    private final Counter requestsFailedCounter;
    private final Counter requestsSuccessfulCounter;
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
        recordsFailedForwardingCounter = pluginMetrics.counter(RECORDS_FAILED_FORWARDING);
        recordsSuccessfullyForwardedCounter = pluginMetrics.counter(RECORDS_SUCCESSFULLY_FORWARDED);
        requestsFailedCounter = pluginMetrics.counter(REQUESTS_FAILED);
        requestsSuccessfulCounter = pluginMetrics.counter(REQUESTS_SUCCESSFUL);
    }

    public CompletableFuture<AggregatedHttpResponse> serializeRecordsAndSendHttpRequest(
            final Collection<Record<Event>> records,
            final String ipAddress,
            final String pluginId,
            final String pipelineName,
            final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer) {
        // TODO: Initialize this in the constructor in future.
        //  It doesn't work right now as default certificate and private key file paths are not valid while loading constructor.
        if (peerClientPool == null) {
            peerClientPool = peerForwarderClientFactory.setPeerClientPool();
        }

        final WebClient client = peerClientPool.getClient(ipAddress);

        final String serializedJsonString = getSerializedJsonString(records, pluginId, pipelineName);

        final CompletableFuture<AggregatedHttpResponse> httpResponseCompletableFuture = clientRequestForwardingLatencyTimer.record(() ->
            processHttpRequest(client, serializedJsonString, records, peerForwarderReceiveBuffer)
        );
        requestsCounter.increment();

        return httpResponseCompletableFuture;
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

    private CompletableFuture<AggregatedHttpResponse> processHttpRequest(final WebClient client, final String content,
                                                                         final Collection<Record<Event>> records,
                                                                         final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer) {
        return CompletableFuture.supplyAsync(() ->
        {
            try {
                final CompletableFuture<AggregatedHttpResponse> aggregate = client.post(DEFAULT_PEER_FORWARDING_URI, content).aggregate();
                return aggregate.join();
            } catch (final Exception e) {
                processFailedRequestsLocally(null, records, peerForwarderReceiveBuffer);
                throw e;
            }
        }, executorService).thenApply(httpResponse -> processFailedRequestsLocally(httpResponse, records, peerForwarderReceiveBuffer));
    }

    private AggregatedHttpResponse processFailedRequestsLocally(final AggregatedHttpResponse httpResponse, final Collection<Record<Event>> records,
                                                                final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer) {
        if (httpResponse == null || httpResponse.status() != HttpStatus.OK) {
            try {
                peerForwarderReceiveBuffer.writeAll(records, FAILED_REQUESTS_BUFFER_WRITE_TIMEOUT_MILLIS);
            } catch (final Exception e) {
                LOG.error("Unable to write failed records to local peer forwarder receive buffer due to exception. Dropping data.", e);
            }

            recordsFailedForwardingCounter.increment(records.size());
            requestsFailedCounter.increment();
        } else {
            recordsSuccessfullyForwardedCounter.increment(records.size());
            requestsSuccessfulCounter.increment();
        }

        return httpResponse;
    }
}