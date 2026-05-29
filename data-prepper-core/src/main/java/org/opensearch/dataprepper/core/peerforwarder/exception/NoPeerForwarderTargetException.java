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
 * This exception is thrown when the peer forwarder receives a request
 * with a destination pipeline or plugin that is not registered.
 */
public class NoPeerForwarderTargetException extends RuntimeException {

    public NoPeerForwarderTargetException(final String errorMessage) {
        super(errorMessage);
    }
}
