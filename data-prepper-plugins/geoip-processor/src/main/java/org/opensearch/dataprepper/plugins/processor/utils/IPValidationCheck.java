/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.opensearch.dataprepper.plugins.processor.exception.InvalidIPAddressException;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Implementation of class for checking IP validation
 * IP should be public it can be either IPV4 or IPV6
 */
public class IPValidationCheck {

    /**
     * Check for IP is valid or not
     * @param ipAddress ipAddress
     * @return boolean
     * @throws  InvalidIPAddressException InvalidIPAddressException
     */
    public static boolean isPublicIpAddress(final String ipAddress) {
        InetAddress address;
        try {
            address = InetAddress.getByName(ipAddress);
        } catch (final UnknownHostException e) {
            throw new InvalidIPAddressException(e.getMessage());
        }
        if (address instanceof Inet6Address || address instanceof Inet4Address) {
            return !address.isSiteLocalAddress() && !address.isLoopbackAddress();
        }
        return false;
    }
}
