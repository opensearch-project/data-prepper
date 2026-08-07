/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import com.google.common.net.InetAddresses;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetAddress;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class GeoInetAddressTest {

    @ParameterizedTest
    @ValueSource(strings = {"93.184.216.34", "172.217.0.0", "142.250.64.0", "2607:f8b0:4005:805::200e"})
    void ipValidationcheckTest_public(String publicIpAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(publicIpAddress);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(true));
        assertThat(actual.get(), equalTo(InetAddresses.forString(publicIpAddress)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.0", "172.16.0.0", "192.168.0.0"})
    void ipValidationcheckTest_negative(String privateIpAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(privateIpAddress);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"123", "255.255.255.999", "true", "[1,2,3]"})
    void ipValidationcheckTest_invalid(String invalidIpAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(invalidIpAddress);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.1", "172.16.0.1", "192.168.1.1"})
    void privateIpAcceptedWhenLookupEnabled(String privateIpAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(privateIpAddress, true);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(true));
        assertThat(actual.get(), equalTo(InetAddresses.forString(privateIpAddress)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.1", "172.16.0.1", "192.168.1.1"})
    void privateIpRejectedWhenLookupDisabled(String privateIpAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(privateIpAddress, false);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.1", "::1"})
    void loopbackAcceptedWhenLookupEnabled(String loopbackAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(loopbackAddress, true);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"93.184.216.34", "2607:f8b0:4005:805::200e"})
    void publicIpWorksInBothModes(String publicIpAddress) {
        final Optional<InetAddress> withFlag = GeoInetAddress.usableInetFromString(publicIpAddress, true);
        final Optional<InetAddress> withoutFlag = GeoInetAddress.usableInetFromString(publicIpAddress, false);
        assertThat(withFlag.isPresent(), equalTo(true));
        assertThat(withoutFlag.isPresent(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"123", "255.255.255.999", "true", "[1,2,3]"})
    void invalidIpRejectedInBothModes(String invalidIpAddress) {
        final Optional<InetAddress> withFlag = GeoInetAddress.usableInetFromString(invalidIpAddress, true);
        final Optional<InetAddress> withoutFlag = GeoInetAddress.usableInetFromString(invalidIpAddress, false);
        assertThat(withFlag.isPresent(), equalTo(false));
        assertThat(withoutFlag.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"224.0.0.1", "239.255.255.250"})
    void multicastAcceptedWhenLookupEnabled(String multicastAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(multicastAddress, true);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"169.254.1.1", "fe80::1"})
    void linkLocalAcceptedWhenLookupEnabled(String linkLocalAddress) {
        final Optional<InetAddress> actual = GeoInetAddress.usableInetFromString(linkLocalAddress, true);
        assertThat(actual, notNullValue());
        assertThat(actual.isPresent(), equalTo(true));
    }
}
