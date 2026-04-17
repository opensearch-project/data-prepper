/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.host;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Provides the hostname of the current Data Prepper instance.
 * This is intended as a shared utility so that hostname resolution
 * is consistent across all components (processors, source coordinators, etc.).
 */
public class HostContext {

    private static final Logger LOG = LoggerFactory.getLogger(HostContext.class);
    private static final String UNKNOWN_HOST = "unknown";
    private static final String HOSTNAME = resolveHostname();

    static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final Exception e) {
            LOG.warn("Failed to resolve hostname, using '{}': {}", UNKNOWN_HOST, e.getMessage());
            return UNKNOWN_HOST;
        }
    }

    /**
     * Returns the hostname of the current Data Prepper host.
     *
     * @return the hostname
     */
    public static String getHostname() {
        return HOSTNAME;
    }
}
