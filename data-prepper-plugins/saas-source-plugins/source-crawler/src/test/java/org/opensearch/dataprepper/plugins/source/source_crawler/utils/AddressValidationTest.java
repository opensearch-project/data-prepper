/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.BadRequestException;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AddressValidationTest {

    @Test
    void testInstanceCreation() {
        assertNotNull(new AddressValidation());
    }

    @Test
    void testGetInetAddress() {
        String testUrl = "https://www.amazon.com";
        System.out.print("Test output");
        System.out.print(AddressValidation.getInetAddress(testUrl));
    }

    @Test
    void testGetInetAddressWithMalformedUrl() {
        String testUrl = "XXXXXXXXXXXXXXXXXXXXXX";
        assertThrows(BadRequestException.class, () -> AddressValidation.getInetAddress(testUrl));
    }

    @Test
    void testGetInetAddressWithUnknownHost() {
        String testUrl = "https://www.thisurldoesntexist1384276t5917278481073.invalid";
        assertThrows(BadRequestException.class, () -> AddressValidation.getInetAddress(testUrl));
    }

    @Test
    void testGetInetAddressWithNullUrl() {
        String testUrl = null;
        assertThrows(BadRequestException.class, () -> AddressValidation.getInetAddress(testUrl));
    }


    @Test
    void testValidateInetAddressAnyLocalAddress() throws UnknownHostException {
        InetAddress wildcardAddress = InetAddress.getByName("0.0.0.0");
        assertThrows(BadRequestException.class, () -> AddressValidation.validateInetAddress(wildcardAddress));
    }

    @Test
    void testValidateInetAddressMulticastAddress() throws UnknownHostException {
        InetAddress multicastAddress = InetAddress.getByName("224.0.0.1");
        assertThrows(BadRequestException.class, () -> AddressValidation.validateInetAddress(multicastAddress));
    }

    @Test
    void testValidateInetAddressLinkLocalAddress() throws UnknownHostException {
        InetAddress linkLocalAddress = InetAddress.getByName("169.254.1.1");
        assertThrows(BadRequestException.class, () -> AddressValidation.validateInetAddress(linkLocalAddress));
    }

    @Test
    void testValidateInetAddressSiteLocalAddress() throws UnknownHostException {
        InetAddress siteLocalAddress = InetAddress.getByName("10.0.0.1");
        assertThrows(BadRequestException.class, () -> AddressValidation.validateInetAddress(siteLocalAddress));
    }

    @Test
    void testValidateInetAddressLoopbackAddress() throws UnknownHostException {
        InetAddress loopbackAddress = InetAddress.getByName("127.0.0.1");
        assertThrows(BadRequestException.class, () -> AddressValidation.validateInetAddress(loopbackAddress));
    }

    @Test
    void testValidateInetAddressValidAddress() throws UnknownHostException, MalformedURLException {
        InetAddress validAddress = InetAddress.getByName(new URL("https://www.amazon.com").getHost());
        assertDoesNotThrow(() -> AddressValidation.validateInetAddress(validAddress));
    }

    @Test
    void testValidateInetAddressNullAddress() {
        InetAddress nullAddress = null;
        assertThrows(NullPointerException.class, () -> AddressValidation.validateInetAddress(nullAddress));
    }

}
