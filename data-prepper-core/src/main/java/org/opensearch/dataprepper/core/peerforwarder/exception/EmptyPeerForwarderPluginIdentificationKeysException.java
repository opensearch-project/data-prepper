/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.exception;

/**
 * This exception is thrown when processor which implements
 * {@link org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding} interface returns an empty set of identification keys.
 *
 * @since 2.0
 */
public class EmptyPeerForwarderPluginIdentificationKeysException extends RuntimeException {

    public EmptyPeerForwarderPluginIdentificationKeysException(final String errorMessage) {
        super(errorMessage);
    }
}
