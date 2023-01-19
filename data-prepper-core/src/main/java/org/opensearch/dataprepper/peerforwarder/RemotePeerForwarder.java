/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.peerforwarder.discovery.StaticPeerListProvider;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
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

class RemotePeerForwarder implements PeerForwarder {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePeerForwarder.class);
    private static final int READ_BATCH_DELAY = 3_000;
    static final String RECORDS_ACTUALLY_PROCESSED_LOCALLY = "recordsActuallyProcessedLocally";
    static final String RECORDS_TO_BE_PROCESSED_LOCALLY = "recordsToBeProcessedLocally";
    static final String RECORDS_TO_BE_FORWARDED = "recordsToBeForwarded";
    static final String RECORDS_SUCCESSFULLY_FORWARDED = "recordsSuccessfullyForwarded";
    static final String RECORDS_FAILED_FORWARDING = "recordsFailedForwarding";
    static final String RECORDS_RECEIVED_FROM_PEERS = "recordsReceivedFromPeers";
    static final String RECORDS_MISSING_IDENTIFICATION_KEYS = "recordsMissingIdentificationKeys";
    static final String REQUESTS_FAILED = "requestsFailed";
    static final String REQUESTS_SUCCESSFUL = "requestsSuccessful";

    private final PeerForwarderClient peerForwarderClient;
    private final HashRing hashRing;
    private final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer;
    private final String pipelineName;
    private final String pluginId;
    private final Set<String> identificationKeys;
    private final Counter recordsActuallyProcessedLocallyCounter;
    private final Counter recordsToBeProcessedLocallyCounter;
    private final Counter recordsToBeForwardedCounter;
    private final Counter recordsSuccessfullyForwardedCounter;
    private final Counter recordsFailedForwardingCounter;
    private final Counter recordsReceivedFromPeersCounter;
    private final Counter recordsMissingIdentificationKeys;
    private final Counter requestsFailedCounter;
    private final Counter requestsSuccessfulCounter;
    private final Integer batchDelay;

    RemotePeerForwarder(final PeerForwarderClient peerForwarderClient,
                        final HashRing hashRing,
                        final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer,
                        final String pipelineName,
                        final String pluginId,
                        final Set<String> identificationKeys,
                        final PluginMetrics pluginMetrics,
                        final Integer batchDelay) {
        this.peerForwarderClient = peerForwarderClient;
        this.hashRing = hashRing;
        this.peerForwarderReceiveBuffer = peerForwarderReceiveBuffer;
        this.pipelineName = pipelineName;
        this.pluginId = pluginId;
        this.identificationKeys = identificationKeys;
        this.batchDelay = batchDelay;
        recordsActuallyProcessedLocallyCounter = pluginMetrics.counter(RECORDS_ACTUALLY_PROCESSED_LOCALLY);
        recordsToBeProcessedLocallyCounter = pluginMetrics.counter(RECORDS_TO_BE_PROCESSED_LOCALLY);
        recordsToBeForwardedCounter = pluginMetrics.counter(RECORDS_TO_BE_FORWARDED);
        recordsSuccessfullyForwardedCounter = pluginMetrics.counter(RECORDS_SUCCESSFULLY_FORWARDED);
        recordsFailedForwardingCounter = pluginMetrics.counter(RECORDS_FAILED_FORWARDING);
        recordsReceivedFromPeersCounter = pluginMetrics.counter(RECORDS_RECEIVED_FROM_PEERS);
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
                AggregatedHttpResponse httpResponse;
                try {
                    httpResponse = peerForwarderClient.serializeRecordsAndSendHttpRequest(entry.getValue(),
                            destinationIp, pluginId, pipelineName);
                } catch (final Exception ex) {
                    httpResponse = null;
                    LOG.warn("Unable to send request to peer, processing locally.", ex);
                }

                if (httpResponse == null || httpResponse.status() != HttpStatus.OK) {
                    recordsToProcessLocally.addAll(entry.getValue());
                    recordsFailedForwardingCounter.increment(entry.getValue().size());
                    requestsFailedCounter.increment();
                } else {
                    recordsSuccessfullyForwardedCounter.increment(entry.getValue().size());
                    requestsSuccessfulCounter.increment();
                }
            }
        }
        recordsActuallyProcessedLocallyCounter.increment(recordsToProcessLocally.size());
        return recordsToProcessLocally;
    }

    public Collection<Record<Event>> receiveRecords() {
        final Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = peerForwarderReceiveBuffer.read(batchDelay);

        final Collection<Record<Event>> records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();

        // Checkpoint the current batch read from the buffer after reading from buffer
        peerForwarderReceiveBuffer.checkpoint(checkpointState);

        recordsReceivedFromPeersCounter.increment(records.size());
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

}
