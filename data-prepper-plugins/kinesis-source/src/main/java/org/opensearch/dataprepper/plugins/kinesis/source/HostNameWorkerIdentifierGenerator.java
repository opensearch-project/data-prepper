/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Generate a unique ID to represent a consumer application instance.
 */
public class HostNameWorkerIdentifierGenerator implements WorkerIdentifierGenerator {

    private static final String hostName;

    static {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * @return Default to use host name.
     */
    @Override
    public String generate() {
        return hostName;
    }
}
