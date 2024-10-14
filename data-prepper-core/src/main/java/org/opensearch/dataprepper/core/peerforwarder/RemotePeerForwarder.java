/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.core.peerforwarder.discovery.StaticPeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

class RemotePeerForwarder implements PeerForwarder {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePeerForwarder.class);

    static final String RECORDS_ACTUALLY_PROCESSED_LOCALLY = "recordsActuallyProcessedLocally";
    static final String RECORDS_TO_BE_PROCESSED_LOCALLY = "recordsToBeProcessedLocally";
    static final String RECORDS_TO_BE_FORWARDED = "recordsToBeForwarded";
    static final String RECORDS_SUCCESSFULLY_FORWARDED = "recordsSuccessfullyForwarded";
    static final String RECORDS_FAILED_FORWARDING = "recordsFailedForwarding";
    static final String RECORDS_MISSING_IDENTIFICATION_KEYS = "recordsMissingIdentificationKeys";
    static final String REQUESTS_FAILED = "requestsFailed";
    static final String REQUESTS_SUCCESSFUL = "requestsSuccessful";

    private final PeerForwarderClient peerForwarderClient;
    private final HashRing hashRing;
    private final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer;
    private final String pipelineName;
    private final String pluginId;
    private final Set<String> identificationKeys;
    final ConcurrentHashMap<String, LinkedBlockingQueue<Record<Event>>> peerBatchingQueueMap;
    private final ConcurrentHashMap<String, Long> peerBatchingLastFlushTimeMap;

    private final Counter recordsActuallyProcessedLocallyCounter;
    private final Counter recordsToBeProcessedLocallyCounter;
    private final Counter recordsToBeForwardedCounter;
    private final Counter recordsSuccessfullyForwardedCounter;
    private final Counter recordsFailedForwardingCounter;
    private final Counter recordsMissingIdentificationKeys;
    private final Counter requestsFailedCounter;
    private final Counter requestsSuccessfulCounter;
    private final Integer batchDelay;
    private final Integer failedForwardingRequestLocalWriteTimeout;
    private final Integer forwardingBatchSize;
    private final Integer forwardingBatchQueueDepth;
    private final Duration forwardingBatchTimeout;
    private final Integer pipelineWorkerThreads;

    RemotePeerForwarder(final PeerForwarderClient peerForwarderClient,
                        final HashRing hashRing,
                        final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer,
                        final String pipelineName,
                        final String pluginId,
                        final Set<String> identificationKeys,
                        final PluginMetrics pluginMetrics,
                        final Integer batchDelay,
                        final Integer failedForwardingRequestLocalWriteTimeout,
                        final Integer forwardingBatchSize,
                        final Integer forwardingBatchQueueDepth,
                        final Duration forwardingBatchTimeout,
                        final Integer pipelineWorkerThreads) {
        this.peerForwarderClient = peerForwarderClient;
        this.hashRing = hashRing;
        this.peerForwarderReceiveBuffer = peerForwarderReceiveBuffer;
        this.pipelineName = pipelineName;
        this.pluginId = pluginId;
        this.identificationKeys = identificationKeys;
        this.batchDelay = batchDelay;
        this.failedForwardingRequestLocalWriteTimeout = failedForwardingRequestLocalWriteTimeout;
        this.forwardingBatchSize = forwardingBatchSize;
        this.forwardingBatchQueueDepth = forwardingBatchQueueDepth;
        this.forwardingBatchTimeout = forwardingBatchTimeout;
        this.pipelineWorkerThreads = pipelineWorkerThreads;
        peerBatchingQueueMap = new ConcurrentHashMap<>();
        peerBatchingLastFlushTimeMap = new ConcurrentHashMap<>();
        
        recordsActuallyProcessedLocallyCounter = pluginMetrics.counter(RECORDS_ACTUALLY_PROCESSED_LOCALLY);
        recordsToBeProcessedLocallyCounter = pluginMetrics.counter(RECORDS_TO_BE_PROCESSED_LOCALLY);
        recordsToBeForwardedCounter = pluginMetrics.counter(RECORDS_TO_BE_FORWARDED);
        recordsSuccessfullyForwardedCounter = pluginMetrics.counter(RECORDS_SUCCESSFULLY_FORWARDED);
        recordsFailedForwardingCounter = pluginMetrics.counter(RECORDS_FAILED_FORWARDING);
        recordsMissingIdentificationKeys = pluginMetrics.counter(RECORDS_MISSING_IDENTIFICATION_KEYS);
        requestsFailedCounter = pluginMetrics.counter(REQUESTS_FAILED);
        requestsSuccessfulCounter = pluginMetrics.counter(REQUESTS_SUCCESSFUL);
    }

    public Collection<Record<Event>> forwardRecords(final Collection<Record<Event>> records) {
        final Map<String, List<Record<Event>>> groupedRecords = groupRecordsBasedOnIdentificationKeys(records, identificationKeys);

        final List<Record<Event>> recordsToProcessLocally = new ArrayList<>();
        for (final Map.Entry<String, List<Record<Event>>> entry : groupedRecords.entrySet()) {
            final String destinationIp = entry.getKey();

            if (isAddressDefinedLocally(destinationIp)) {
                recordsToProcessLocally.addAll(entry.getValue());
                recordsToBeProcessedLocallyCounter.increment(entry.getValue().size());
            } else {
                recordsToBeForwardedCounter.increment(entry.getValue().size());
                final List<Record<Event>> recordsFailedToBatch = batchRecordsForForwarding(destinationIp, entry.getValue());
                recordsToProcessLocally.addAll(recordsFailedToBatch);
            }
        }

        forwardBatchedRecords();
        recordsActuallyProcessedLocallyCounter.increment(recordsToProcessLocally.size());

        return recordsToProcessLocally;
    }

    public Collection<Record<Event>> receiveRecords() {
        final Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = peerForwarderReceiveBuffer.read(batchDelay);

        final Collection<Record<Event>> records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();

        // Checkpoint the current batch read from the buffer after reading from buffer
        peerForwarderReceiveBuffer.checkpoint(checkpointState);

        return records;
    }

    private Map<String, List<Record<Event>>> groupRecordsBasedOnIdentificationKeys(
            final Collection<Record<Event>> records,
            final Set<String> identificationKeys
    ) {
        final Map<String, List<Record<Event>>> groupedRecords = new HashMap<>();

        // group records based on IP address calculated by HashRing
        for (final Record<Event> record : records) {
            final Event event = record.getData();

            final List<String> identificationKeyValues = new LinkedList<>();
            int numMissingIdentificationKeys = 0;
            for (final String identificationKey : identificationKeys) {
                final Object identificationKeyValue = event.get(identificationKey, Object.class);
                if (identificationKeyValue == null) {
                    identificationKeyValues.add(null);
                    numMissingIdentificationKeys++;
                } else {
                    identificationKeyValues.add(identificationKeyValue.toString());
                }
            }
            if (numMissingIdentificationKeys == identificationKeys.size()) {
                recordsMissingIdentificationKeys.increment(1);
                identificationKeyValues.clear();
            }

            final String dataPrepperIp = hashRing.getServerIp(identificationKeyValues).orElse(StaticPeerListProvider.LOCAL_ENDPOINT);
            groupedRecords.computeIfAbsent(dataPrepperIp, x -> new ArrayList<>()).add(record);
        }
        return groupedRecords;
    }

    private boolean isAddressDefinedLocally(final String address) {
        final InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(address);
        } catch (final UnknownHostException e) {
            return false;
        }
        if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress()) {
            return true;
        } else {
            try {
                return NetworkInterface.getByInetAddress(inetAddress) != null;
            } catch (final SocketException e) {
                return false;
            }
        }
    }

    private List<Record<Event>> batchRecordsForForwarding(final String destinationIp, final List<Record<Event>> records) {
        try {
            final List<Record<Event>> recordsFailedToBatch = populateBatchingQueue(destinationIp, records);
            if (!recordsFailedToBatch.isEmpty()) {
                recordsFailedForwardingCounter.increment(recordsFailedToBatch.size());
            }

            return recordsFailedToBatch;
        } catch (final Exception ex) {
            LOG.warn("Unable to batch records for forwarding, processing locally.", ex);
            recordsFailedForwardingCounter.increment(records.size());
            return records;
        }
    }

    private List<Record<Event>> populateBatchingQueue(final String destinationIp, final List<Record<Event>> records) {
        peerBatchingQueueMap.putIfAbsent(destinationIp, new LinkedBlockingQueue<>(forwardingBatchSize * pipelineWorkerThreads * forwardingBatchQueueDepth));
        peerBatchingLastFlushTimeMap.putIfAbsent(destinationIp, System.currentTimeMillis());

        final List<Record<Event>> recordsFailedToBatch = new ArrayList<>();
        final LinkedBlockingQueue<Record<Event>> peerBatchingQueue = peerBatchingQueueMap.get(destinationIp);
        for (final Record<Event> record: records) {
            try {
                peerBatchingQueue.add(record);
            } catch (final IllegalStateException e) {
                recordsFailedToBatch.add(record);
            }
        }

        final int numberOfRecordsFailedToBatch = recordsFailedToBatch.size();
        if (numberOfRecordsFailedToBatch > 0) {
            LOG.warn("Failed to add {} records to the batching queue, processing locally.", numberOfRecordsFailedToBatch);
        }
        return recordsFailedToBatch;
    }

    private void forwardBatchedRecords() {
        final Map<CompletableFuture<AggregatedHttpResponse>, List<Record<Event>>> futuresMap = new HashMap<>();
        peerBatchingQueueMap.forEach((ipAddress, records) -> {
            futuresMap.putAll(forwardRecordsForIp(ipAddress));
        });

        final CompletableFuture<Void> compositeFuture = CompletableFuture.allOf(futuresMap.keySet().toArray(CompletableFuture[]::new));
        try {
            compositeFuture.get();
        } catch (final ExecutionException | InterruptedException e) {
            LOG.debug("Caught exception getting results of composite forwarding future.", e);
        }

        futuresMap.forEach((key, value) -> {
            AggregatedHttpResponse httpResponse;
            try {
                httpResponse = key.get();
            } catch (final ExecutionException | InterruptedException e) {
                LOG.warn("Unable to send request to peer, processing locally.", e);
                httpResponse = null;
            }

            processFailedRequestsLocally(httpResponse, value);
        });
    }

    private Map<CompletableFuture<AggregatedHttpResponse>, List<Record<Event>>> forwardRecordsForIp(final String destinationIp) {
        final Map<CompletableFuture<AggregatedHttpResponse>, List<Record<Event>>> forwardingRequestsMap = new HashMap<>();

        List<Record<Event>> recordsToForward = getRecordsToForward(destinationIp);
        while (!recordsToForward.isEmpty()) {
            try {
                final CompletableFuture<AggregatedHttpResponse> responseFuture =
                        peerForwarderClient.serializeRecordsAndSendHttpRequest(recordsToForward, destinationIp, pluginId, pipelineName);
                forwardingRequestsMap.put(responseFuture, recordsToForward);
                for (Record<Event> record: recordsToForward) {
                    Event event = record.getData();
                    event.getEventHandle().release(true);
                }
            } catch (final Exception e) {
                LOG.warn("Unable to submit request for forwarding, processing locally.", e);
                processFailedRequestsLocally(null, recordsToForward);
            }
            recordsToForward = getRecordsToForward(destinationIp);
        }

        return forwardingRequestsMap;
    }

    private List<Record<Event>> getRecordsToForward(final String destinationIp) {
        if (shouldFlushBatch(destinationIp)) {
            peerBatchingLastFlushTimeMap.put(destinationIp, System.currentTimeMillis());

            final List<Record<Event>> recordsToForward = new ArrayList<>();
            peerBatchingQueueMap.get(destinationIp).drainTo(recordsToForward, forwardingBatchSize);

            return recordsToForward;
        }

        return Collections.emptyList();
    }

    private boolean shouldFlushBatch(final String destinationIp) {
        final long currentTime = System.currentTimeMillis();
        final long millisSinceLastFlush = currentTime - peerBatchingLastFlushTimeMap.getOrDefault(destinationIp, System.currentTimeMillis());
        final Duration durationSinceLastFlush = Duration.of(millisSinceLastFlush, ChronoUnit.MILLIS);

        final boolean shouldFlushDueToTimeout = durationSinceLastFlush.compareTo(forwardingBatchTimeout) >= 0;
        return shouldFlushDueToTimeout || peerBatchingQueueMap.get(destinationIp).size() >= forwardingBatchSize;
    }

    void processFailedRequestsLocally(final AggregatedHttpResponse httpResponse, final Collection<Record<Event>> records) {
        if (httpResponse == null || httpResponse.status() != HttpStatus.OK) {
            try {
                peerForwarderReceiveBuffer.writeAll(records, failedForwardingRequestLocalWriteTimeout);
                recordsActuallyProcessedLocallyCounter.increment(records.size());
            } catch (final Exception e) {
                LOG.error("Unable to write failed records to local peer forwarder receive buffer due to {}. Dropping {} records.", e.getMessage(), records.size());
            }

            recordsFailedForwardingCounter.increment(records.size());
            requestsFailedCounter.increment();
        } else {
            recordsSuccessfullyForwardedCounter.increment(records.size());
            requestsSuccessfulCounter.increment();
        }
    }
}
