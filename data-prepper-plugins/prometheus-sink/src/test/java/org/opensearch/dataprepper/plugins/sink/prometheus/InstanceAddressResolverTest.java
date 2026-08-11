/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class InstanceAddressResolverTest {

    private static final String ENDPOINT = "http://198.51.100.7:9090/api/v1/write";

    // RFC 2606 reserves .invalid, so this host cannot resolve and the routing lookup cannot answer.
    private static final String UNRESOLVABLE_ENDPOINT = "http://endpoint.invalid:9090/api/v1/write";

    @Test
    void resolves_an_ipv4_address_on_this_machine() {
        assumeTrue(hasAnAddress(), "this machine exposes no non-loopback IPv4 address");

        assertThat(InstanceAddressResolver.resolveLocalIpv4(ENDPOINT),
                matchesPattern("\\d{1,3}(\\.\\d{1,3}){3}"));
    }

    @Test
    void returns_the_same_address_on_every_call() {
        assumeTrue(hasAnAddress(), "this machine exposes no non-loopback IPv4 address");

        assertThat(InstanceAddressResolver.resolveLocalIpv4(ENDPOINT),
                equalTo(InstanceAddressResolver.resolveLocalIpv4(ENDPOINT)));
    }

    @Test
    void falls_back_to_the_interface_list_when_the_endpoint_cannot_be_resolved() {
        assumeTrue(hasAnAddress(), "this machine exposes no non-loopback IPv4 address");

        assertThat(InstanceAddressResolver.resolveViaRouteTo(UNRESOLVABLE_ENDPOINT), nullValue());
        assertThat(InstanceAddressResolver.resolveLocalIpv4(UNRESOLVABLE_ENDPOINT),
                matchesPattern("\\d{1,3}(\\.\\d{1,3}){3}"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "not a url", "/api/v1/write", "http://"})
    void the_routing_lookup_declines_a_url_it_cannot_use(final String url) {
        assertThat(InstanceAddressResolver.resolveViaRouteTo(url), nullValue());
    }

    @Test
    void the_routing_lookup_picks_an_address_which_is_assigned_to_this_machine() throws Exception {
        final String routed = InstanceAddressResolver.resolveViaRouteTo(ENDPOINT);
        assumeTrue(routed != null, "this machine has no route to the test endpoint");

        // The address has to be one of this machine's own, not merely well formed.
        assertThat(routed, in(localAddresses()));
    }

    @Test
    void throws_rather_than_returning_a_placeholder_when_no_interface_has_an_address() {
        final Enumeration<NetworkInterface> noInterfaces = Collections.enumeration(Collections.emptyList());

        final InvalidPluginConfigurationException exception = assertThrows(
                InvalidPluginConfigurationException.class,
                () -> InstanceAddressResolver.resolveLocalIpv4(() -> noInterfaces));

        assertThat(exception.getMessage(), containsString("Unable to resolve an IPv4 address"));
        // The setting is named so an operator reading the startup failure knows which option caused it.
        assertThat(exception.getMessage(), containsString("instance_label"));
    }

    @Test
    void throws_when_the_interfaces_cannot_be_read() {
        final InvalidPluginConfigurationException exception = assertThrows(
                InvalidPluginConfigurationException.class,
                () -> InstanceAddressResolver.resolveLocalIpv4(() -> {
                    throw new SocketException("no interfaces");
                }));

        assertThat(exception.getMessage(), containsString("Unable to read the local network interfaces"));
        assertThat(exception.getMessage(), containsString("instance_label"));
    }

    private static boolean hasAnAddress() {
        try {
            InstanceAddressResolver.resolveLocalIpv4(ENDPOINT);
            return true;
        } catch (final InvalidPluginConfigurationException e) {
            return false;
        }
    }

    private static List<String> localAddresses() throws SocketException {
        final List<String> addresses = new ArrayList<>();
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces != null && interfaces.hasMoreElements()) {
            final Enumeration<InetAddress> onInterface = interfaces.nextElement().getInetAddresses();
            while (onInterface.hasMoreElements()) {
                addresses.add(onInterface.nextElement().getHostAddress());
            }
        }
        return addresses;
    }
}
