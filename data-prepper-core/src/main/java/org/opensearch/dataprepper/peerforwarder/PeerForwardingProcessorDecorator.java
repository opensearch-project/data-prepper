/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.opensearch.dataprepper.peerforwarder.exception.EmptyPeerForwarderPluginIdentificationKeysException;
import org.opensearch.dataprepper.peerforwarder.exception.UnsupportedPeerForwarderPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

public class PeerForwardingProcessorDecorator implements Processor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwardingProcessorDecorator.class);

    private final Processor innerProcessor;
    private final PeerForwarder peerForwarder;
    private final String pluginId;
    private final Set<String> identificationKeys;

    public PeerForwardingProcessorDecorator(final Processor innerProcessor,
                                            final PeerForwarderProvider peerForwarderProvider,
                                            final String pipelineName,
                                            final String pluginId) {
        this.innerProcessor = innerProcessor;
        this.pluginId = pluginId;

        if (innerProcessor instanceof RequiresPeerForwarding) {
            identificationKeys = ((RequiresPeerForwarding) innerProcessor).getIdentificationKeys();
        } else {
            throw new UnsupportedPeerForwarderPluginException("Peer Forwarding is only supported for plugins which implement RequiresPeerForwarding interface.");
        }
        if (identificationKeys.isEmpty()) {
            throw new EmptyPeerForwarderPluginIdentificationKeysException("Peer Forwarder Plugin: %s cannot have empty identification keys." + pluginId);
        }
        this.peerForwarder = peerForwarderProvider.register(pipelineName, pluginId, identificationKeys);
        // TODO: remove this log message after implementing peer forwarder
        LOG.info("Peer Forwarder not implemented yet, processing events locally.");
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        final Collection<Record<Event>> recordsToProcessLocally = peerForwarder.forwardRecords(records);

        return innerProcessor.execute(recordsToProcessLocally);
    }

    @Override
    public void prepareForShutdown() {
        innerProcessor.prepareForShutdown();
    }

    @Override
    public boolean isReadyForShutdown() {
        return innerProcessor.isReadyForShutdown();
    }

    @Override
    public void shutdown() {
        innerProcessor.shutdown();
    }
}
