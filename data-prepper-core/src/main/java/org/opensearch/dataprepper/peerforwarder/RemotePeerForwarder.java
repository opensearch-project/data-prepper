/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
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


    private final PeerForwarderClient peerForwarderClient;
    private final HashRing hashRing;
    private final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer;
    private final String pipelineName;
    private final String pluginId;
    private final Set<String> identificationKeys;

    RemotePeerForwarder(final PeerForwarderClient peerForwarderClient,
                        final HashRing hashRing,
                        final PeerForwarderReceiveBuffer<Record<Event>> peerForwarderReceiveBuffer,
                        final String pipelineName,
                        final String pluginId,
                        final Set<String> identificationKeys) {
        this.peerForwarderClient = peerForwarderClient;
        this.hashRing = hashRing;
        this.peerForwarderReceiveBuffer = peerForwarderReceiveBuffer;
        this.pipelineName = pipelineName;
        this.pluginId = pluginId;
        this.identificationKeys = identificationKeys;
    }

    public Collection<Record<Event>> forwardRecords(final Collection<Record<Event>> records) {
        final Map<String, List<Record<Event>>> groupedRecords = groupRecordsBasedOnIdentificationKeys(records, identificationKeys);

        final List<Record<Event>> recordsToProcessLocally = new ArrayList<>();

        for (final Map.Entry<String, List<Record<Event>>> entry : groupedRecords.entrySet()) {
            final String destinationIp = entry.getKey();

            if (isAddressDefinedLocally(destinationIp)) {
                recordsToProcessLocally.addAll(entry.getValue());
            } else {
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
                }
            }
        }
        return recordsToProcessLocally;
    }

    public Collection<Record<Event>> receiveRecords() {
        // TODO: Make the read timeout configurable? This is the default read batch delay in PipelineConfiguration
        final Map.Entry<Collection<Record<Event>>, CheckpointState> readResult =
                peerForwarderReceiveBuffer.read(READ_BATCH_DELAY);

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
            for (final String identificationKey : identificationKeys) {
                identificationKeyValues.add(event.get(identificationKey, Object.class).toString());
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

    @Override
    public boolean isReadyForShutdown() {
        return peerForwarderReceiveBuffer.isEmpty();
    }

}
