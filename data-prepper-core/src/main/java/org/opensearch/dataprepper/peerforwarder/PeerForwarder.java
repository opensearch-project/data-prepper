/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import org.opensearch.dataprepper.peerforwarder.discovery.StaticPeerListProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PeerForwarder {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarder.class);
    private static final WebClient LOCAL_CLIENT = null;
    public static final int ASYNC_REQUEST_THREAD_COUNT = 200;

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final ObjectMapper objectMapper;
    private HashRing hashRing;
    private PeerClientPool peerClientPool;
    private final ExecutorService executorService;

    public PeerForwarder(final PeerForwarderClientFactory peerForwarderClientFactory,
                         final ObjectMapper objectMapper) {
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.objectMapper = objectMapper;

        executorService = Executors.newFixedThreadPool(ASYNC_REQUEST_THREAD_COUNT);
    }

    public List<Record<Event>> processRecords(final Collection<Record<Event>> records, final Set<String> identificationKeys) {
        // TODO: decide the default behaviour of Core Peer Forwarder and move the HashRing and PeerClientPool creation to constructor
        hashRing = peerForwarderClientFactory.createHashRing();
        peerClientPool = peerForwarderClientFactory.setPeerClientPool();

        final Map<String, List<Record<Event>>> groupedRecords = groupRecordsBasedOnIdentificationKeys(records, identificationKeys);

        final List<Record<Event>> recordsToProcessLocally = new ArrayList<>();

        for (final Map.Entry<String, List<Record<Event>>> entry: groupedRecords.entrySet()) {
            final WebClient client = getClient(entry.getKey());

            String serializedJsonString;

            if  (isLocalClient(client)) {
                recordsToProcessLocally.addAll(entry.getValue());
            }
            else {
                try {
                    serializedJsonString = serializeRecordsToJsonString(entry.getValue());
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                final HttpRequest httpRequest = createHttpRequest(serializedJsonString);
                final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseCompletableFuture = forwardRecords(client, httpRequest);

                try {
                    final AggregatedHttpResponse response = aggregatedHttpResponseCompletableFuture.get();
                    if (response == null || response.status() != HttpStatus.OK) {
                        recordsToProcessLocally.addAll(entry.getValue());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Problem with asynchronous peer forwarding", e);
                }
            }
        }
        return recordsToProcessLocally;
    }

    private Map<String, List<Record<Event>>> groupRecordsBasedOnIdentificationKeys(Collection<Record<Event>> records, Set<String> identificationKeys) {
        final Map<String, List<Record<Event>>> groupedRecords = new HashMap<>();

        // group records based on IP address calculated by HashRing
        for (final Record<Event> record : records) {
            final Event event = record.getData();

            List<String> identificationKeyValues = new LinkedList<>();
            for (final String identificationKey : identificationKeys) {
                identificationKeyValues.add(event.get(identificationKey, Object.class).toString());
            }

            final String dataPrepperIp = hashRing.getServerIp(identificationKeyValues).orElse(StaticPeerListProvider.LOCAL_ENDPOINT);
            groupedRecords.computeIfAbsent(dataPrepperIp, x -> new ArrayList<>()).add(record);
        }
        return groupedRecords;
    }

    private WebClient getClient(final String address) {
        return isAddressDefinedLocally(address) ? LOCAL_CLIENT : peerClientPool.getClient(address);
    }

    private boolean isAddressDefinedLocally(final String address) {
        final InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            return false;
        }
        if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress()) {
            return true;
        } else {
            try {
                return NetworkInterface.getByInetAddress(inetAddress) != null;
            } catch (SocketException e) {
                return false;
            }
        }
    }

    private boolean isLocalClient(final WebClient client) {
        return client == LOCAL_CLIENT;
    }

    private String serializeRecordsToJsonString(final List<Record<Event>> records) throws JsonProcessingException {
        List<SerializedEvent> serializedEvents = new ArrayList<>();

        for (Record<Event> record: records) {
            final Event event = record.getData();
            serializedEvents.add(new SerializedEvent(event.getMetadata().getEventType(), event.toJsonString()));
        }

        // TODO: get plugin id from PipelineParser
        final WireEvents wireEvents = new WireEvents(serializedEvents, "pluginId");

        return objectMapper.writeValueAsString(wireEvents);
    }

    private HttpRequest createHttpRequest(final String content) {
        return HttpRequest.builder()
                .content(content)
                .method(HttpMethod.POST)
                .build();
    }

    private CompletableFuture<AggregatedHttpResponse> forwardRecords(final WebClient client, final HttpRequest httpRequest) {

        final String authority = client.uri().getAuthority();

        return CompletableFuture.supplyAsync(() ->
        {
            try {
                return client.execute(httpRequest).aggregate().join();
            } catch (Exception e) {
                LOG.error("Failed to forward request to address: {}", authority, e);
                return null;
            }
        }, executorService);
    }
}
