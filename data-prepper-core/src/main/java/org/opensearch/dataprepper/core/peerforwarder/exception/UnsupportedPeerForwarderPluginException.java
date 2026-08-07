/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
