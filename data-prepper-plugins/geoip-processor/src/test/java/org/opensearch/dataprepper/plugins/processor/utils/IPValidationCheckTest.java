/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.UnknownHostException;

@ExtendWith(MockitoExtension.class)
class IPValidationCheckTest {

    private static final String PRIVATE_IP_ADDRESS = "192.168.29.233";
    private static final String PUBLIC_IP_ADDRESS = "2001:4860:4860::8888";
    private static final String INVALID_IP_ADDRESS = "255.255.255.0";

    @Test
    void ipValidationcheckTest_positive() throws UnknownHostException {
        Assertions.assertTrue(IPValidationCheck.isPublicIpAddress(PUBLIC_IP_ADDRESS));
    }

    @Test
    void ipValidationcheckTest_negative() throws UnknownHostException {
        Assertions.assertFalse(IPValidationCheck.isPublicIpAddress(PRIVATE_IP_ADDRESS));
    }

    @Test
    void ipValidationcheckTest_invalid() throws UnknownHostException {
        Assertions.assertTrue(IPValidationCheck.isPublicIpAddress(INVALID_IP_ADDRESS));
    }
}