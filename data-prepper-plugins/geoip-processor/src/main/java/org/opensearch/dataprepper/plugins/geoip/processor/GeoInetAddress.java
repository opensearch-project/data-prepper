/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import com.google.common.net.InetAddresses;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Optional;

/**
 * Implementation of class for checking IP validation
 * IP should be public it can be either IPV4 or IPV6
 */
class GeoInetAddress {

    static Optional<InetAddress> usableInetFromString(final String ipAddress) {
        final InetAddress address;
        try {
            address = InetAddresses.forString(ipAddress);
        } catch (final IllegalArgumentException e) {
            return Optional.empty();
        }
        if (isPublicIpAddress(address))
            return Optional.of(address);
        return Optional.empty();
    }

    private static boolean isPublicIpAddress(final InetAddress ipAddress) {
        if (ipAddress instanceof Inet6Address || ipAddress instanceof Inet4Address) {
            return !ipAddress.isSiteLocalAddress() && !ipAddress.isLoopbackAddress();
        }
        return false;
    }
}
