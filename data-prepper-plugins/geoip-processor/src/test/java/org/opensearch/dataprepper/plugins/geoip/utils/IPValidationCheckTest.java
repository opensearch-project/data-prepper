/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IPValidationCheckTest {

    private static final String PRIVATE_IP_ADDRESS = "192.168.29.233";
    private static final String PUBLIC_IP_ADDRESS = "2001:4860:4860::8888";
    private static final String INVALID_IP_ADDRESS = "255.255.255.999";

    @Test
    void ipValidationcheckTest_public() {
        Assertions.assertTrue(IPValidationCheck.isPublicIpAddress(PUBLIC_IP_ADDRESS));
    }

    @Test
    void ipValidationcheckTest_negative() {
        Assertions.assertFalse(IPValidationCheck.isPublicIpAddress(PRIVATE_IP_ADDRESS));
    }

    @Test
    void ipValidationcheckTest_invalid() {
        Assertions.assertFalse(IPValidationCheck.isPublicIpAddress(INVALID_IP_ADDRESS));
    }
}