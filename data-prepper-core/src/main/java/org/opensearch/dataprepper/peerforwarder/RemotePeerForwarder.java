/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import org.opensearch.dataprepper.peerforwarder.discovery.StaticPeerListProvider;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;

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
    private final PeerForwarderClient peerForwarderClient;
    private final HashRing hashRing;
    private final String pipelineName;
    private final String pluginId;
    private final Set<String> identificationKeys;

    RemotePeerForwarder(final PeerForwarderClient peerForwarderClient,
                        final HashRing hashRing,
                        final String pipelineName,
                        final String pluginId,
                        final Set<String> identificationKeys) {
        this.peerForwarderClient = peerForwarderClient;
        this.hashRing = hashRing;
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
                final AggregatedHttpResponse httpResponse = peerForwarderClient.serializeRecordsAndSendHttpRequest(entry.getValue(),
                        destinationIp, pluginId);
                if (httpResponse == null || httpResponse.status() != HttpStatus.OK) {
                    recordsToProcessLocally.addAll(entry.getValue());
                }
            }
        }
        return recordsToProcessLocally;
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

}
