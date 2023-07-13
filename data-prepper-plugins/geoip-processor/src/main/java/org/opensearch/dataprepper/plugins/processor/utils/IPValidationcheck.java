/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Implementation of class for checking IP validation
 * IP should be public it can be either IPV4 or IPV6
 */
public class IPValidationcheck {

    /**
     * Check for IP is valid or not
     * @param input input
     * @return boolean
     * @throws  UnknownHostException UnknownHostException
     */
    public static boolean isPublicIpAddress(String input) throws UnknownHostException {
        String ipAddress = input;
        InetAddress address = InetAddress.getByName(input);
        if (address instanceof Inet6Address || address instanceof Inet4Address) {
            return !address.isSiteLocalAddress() && !address.isLoopbackAddress();
        }
        return false;
    }
}
