/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

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
    public static boolean ipValidationcheck(String input) throws UnknownHostException {
       //TODO : call IP validation logic
        return false;
    }

    /**
     * Checking for the IP is valid or not if IP is not a private IPV4 and IPV6
     * @param ipAddress ipAddress
     * @return boolean
     * @throws UnknownHostException UnknownHostException
     */
    public static boolean PrivateIPVersionCheck(String ipAddress) throws UnknownHostException {
        //TODO : Checking for the IP is valid or not if IP is not a private IPV4 and IPV6
        return false;
    }
}
