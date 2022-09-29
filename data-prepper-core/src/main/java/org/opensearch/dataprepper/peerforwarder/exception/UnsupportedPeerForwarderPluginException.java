/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.exception;

/**
 * This exception is thrown when processor which doesn't implement
 * {@link org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding} interface is passed to
 * {@link org.opensearch.dataprepper.peerforwarder.PeerForwardingProcessorDecorator}.
 *
 * @since 2.0
 */
public class UnsupportedPeerForwarderPluginException extends RuntimeException {

    public UnsupportedPeerForwarderPluginException(final String errorMessage) {
        super(errorMessage);
    }
}
