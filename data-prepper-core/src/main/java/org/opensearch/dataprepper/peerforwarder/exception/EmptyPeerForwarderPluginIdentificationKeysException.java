/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.exception;

public class EmptyPeerForwarderPluginIdentificationKeysException extends RuntimeException {

    public EmptyPeerForwarderPluginIdentificationKeysException(final String errorMessage) {
        super(errorMessage);
    }
}
