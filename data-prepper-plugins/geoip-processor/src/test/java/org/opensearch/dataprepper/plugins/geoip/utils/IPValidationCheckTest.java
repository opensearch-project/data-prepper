/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.utils;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class IPValidationCheckTest {

    @ParameterizedTest
    @ValueSource(strings = {"93.184.216.34", "172.217.0.0", "142.250.64.0", "2607:f8b0:4005:805::200e"})
    void ipValidationcheckTest_public(String publicIpAddress) {
        assertThat(IPValidationCheck.isPublicIpAddress(publicIpAddress), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.0", "172.16.0.0", "192.168.0.0"})
    void ipValidationcheckTest_negative(String privateIpAddress) {
        assertThat(IPValidationCheck.isPublicIpAddress(privateIpAddress), is(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"123", "255.255.255.999", "true", "[1,2,3]"})
    void ipValidationcheckTest_invalid(String invalidIpAddress) {
        assertThat(IPValidationCheck.isPublicIpAddress(invalidIpAddress), is(false));
    }
}