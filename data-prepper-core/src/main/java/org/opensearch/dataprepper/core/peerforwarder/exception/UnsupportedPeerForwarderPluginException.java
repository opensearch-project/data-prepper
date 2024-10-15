/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.exception;

import org.opensearch.dataprepper.core.peerforwarder.PeerForwardingProcessorDecorator;

/**
 * This exception is thrown when processor which doesn't implement
 * {@link org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding} interface is passed to
 * {@link PeerForwardingProcessorDecorator}.
 *
 * @since 2.0
 */
public class UnsupportedPeerForwarderPluginException extends RuntimeException {

    public UnsupportedPeerForwarderPluginException(final String errorMessage) {
        super(errorMessage);
    }
}
