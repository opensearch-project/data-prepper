/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.host;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HostContextTest {

    @Test
    void getHostname_returns_non_null_non_empty_value() {
        final String hostname = HostContext.getHostname();
        assertThat(hostname, notNullValue());
        assertThat(hostname, not(emptyString()));
    }

    @Test
    void getHostname_returns_consistent_value() {
        assertThat(HostContext.getHostname(), equalTo(HostContext.getHostname()));
    }

    @Test
    void isHostnameResolved_agrees_with_the_resolved_hostname() {
        assertThat(HostContext.isHostnameResolved(),
                equalTo(HostContext.isHostnameResolved(HostContext.getHostname())));
    }

    @Test
    void isHostnameResolved_is_false_only_for_the_unknown_placeholder() {
        assertThat(HostContext.isHostnameResolved(HostContext.UNKNOWN_HOST), is(false));
        assertThat(HostContext.isHostnameResolved("some-host"), is(true));
    }

    @Test
    void constructor_can_be_created() {
        assertThat(new HostContext(), notNullValue());
    }

    // The fallback chain.

    @Test
    void resolveHostname_prefers_the_local_host() {
        assertThat(HostContext.resolveHostname(() -> "from-local-host", () -> "from-env",
                () -> "from-interfaces", () -> "from-route"), equalTo("from-local-host"));
    }

    @Test
    void resolveHostname_falls_back_to_the_environment() {
        assertThat(HostContext.resolveHostname(() -> null, () -> "from-env",
                () -> "from-interfaces", () -> "from-route"), equalTo("from-env"));
    }

    @Test
    void resolveHostname_falls_back_to_the_network_interfaces() {
        assertThat(HostContext.resolveHostname(() -> null, () -> null,
                () -> "from-interfaces", () -> "from-route"), equalTo("from-interfaces"));
    }

    @Test
    void resolveHostname_falls_back_to_the_routing_lookup() {
        assertThat(HostContext.resolveHostname(() -> null, () -> null,
                () -> null, () -> "from-route"), equalTo("from-route"));
    }

    @Test
    void resolveHostname_returns_unknown_when_every_strategy_fails() {
        assertThat(HostContext.resolveHostname(() -> null, () -> null, () -> null, () -> null),
                equalTo(HostContext.UNKNOWN_HOST));
    }

    @Test
    void resolveHostname_uses_the_real_strategies_and_resolves_something() {
        assertThat(HostContext.resolveHostname(), notNullValue());
    }

    // Local host.

    @Test
    void resolveFromLocalHost_returns_the_local_hostname() throws UnknownHostException {
        assertThat(HostContext.resolveFromLocalHost(),
                equalTo(InetAddress.getLocalHost().getHostName()));
    }

    @Test
    void resolveFromLocalHost_returns_null_when_the_lookup_fails() {
        assertThat(HostContext.resolveFromLocalHost(() -> {
            throw new UnknownHostException("no local host");
        }), nullValue());
    }

    @Test
    void resolveFromLocalHost_returns_null_for_an_unusable_hostname() throws UnknownHostException {
        final InetAddress blankNamed = InetAddress.getByAddress("   ", new byte[]{10, 1, 2, 3});
        assertThat(HostContext.resolveFromLocalHost(() -> blankNamed), nullValue());
    }

    // Environment.

    @Test
    void resolveFromEnvironment_reads_the_hostname_variable() {
        final Map<String, String> environment = new HashMap<>();
        environment.put(HostContext.HOSTNAME_ENVIRONMENT_VARIABLE, "container-1234");
        assertThat(HostContext.resolveFromEnvironment(environment::get), equalTo("container-1234"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void resolveFromEnvironment_rejects_a_blank_value(final String value) {
        assertThat(HostContext.resolveFromEnvironment(name -> value), nullValue());
    }

    @Test
    void resolveFromEnvironment_returns_null_when_the_variable_is_absent() {
        assertThat(HostContext.resolveFromEnvironment(name -> null), nullValue());
    }

    @Test
    void resolveFromEnvironment_reads_the_real_environment() {
        final String fromEnvironment = System.getenv(HostContext.HOSTNAME_ENVIRONMENT_VARIABLE);
        final String expected = fromEnvironment == null || fromEnvironment.trim().isEmpty()
                ? null : fromEnvironment.trim();
        assertThat(HostContext.resolveFromEnvironment(), equalTo(expected));
    }

    // Network interfaces.

    @Test
    void resolveFromNetworkInterfaces_returns_null_when_no_interface_has_an_address() {
        final Enumeration<NetworkInterface> none = Collections.enumeration(Collections.emptyList());
        assertThat(HostContext.resolveFromNetworkInterfaces(() -> none), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_returns_null_when_the_interfaces_cannot_be_read() {
        assertThat(HostContext.resolveFromNetworkInterfaces(() -> {
            throw new SocketException("no interfaces");
        }), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_returns_null_when_the_enumeration_is_null() {
        assertThat(HostContext.resolveFromNetworkInterfaces(() -> null), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_skips_an_interface_whose_state_cannot_be_read() throws SocketException {
        final NetworkInterface unreadable = mock(NetworkInterface.class);
        when(unreadable.isUp()).thenThrow(new SocketException("interface went away"));
        final Enumeration<NetworkInterface> onlyUnreadable =
                Collections.enumeration(Collections.singletonList(unreadable));

        assertThat(HostContext.resolveFromNetworkInterfaces(() -> onlyUnreadable), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_walks_the_real_interfaces() throws SocketException {
        final String address = HostContext.resolveFromNetworkInterfaces(
                NetworkInterface::getNetworkInterfaces);
        assumeTrue(address != null, "this machine exposes no usable address on any interface");

        // Loopback is always present and must never be chosen.
        assertThat(address, not(equalTo("127.0.0.1")));
        assertThat(localAddresses().contains(address), is(true));
    }

    @Test
    void resolveFromNetworkInterfaces_returns_the_same_address_on_every_call() {
        assertThat(HostContext.resolveFromNetworkInterfaces(),
                equalTo(HostContext.resolveFromNetworkInterfaces()));
    }

    // Routing lookup.

    @Test
    void resolveFromRouteLookup_returns_an_address_assigned_to_this_machine() throws SocketException {
        final String address = HostContext.resolveFromRouteLookup();
        assumeTrue(address != null, "this machine has no route to the lookup address");

        assertThat(localAddresses().contains(address), is(true));
    }

    @Test
    void resolveFromRouteLookup_returns_null_when_the_socket_cannot_be_opened() {
        assertThat(HostContext.resolveFromRouteLookup(() -> {
            throw new SocketException("no socket");
        }), nullValue());
    }

    @Test
    void resolveFromRouteLookup_returns_null_when_the_socket_has_no_usable_address() throws Exception {
        // A socket bound to loopback reports a loopback source address, which cannot distinguish hosts.
        assertThat(HostContext.resolveFromRouteLookup(
                () -> new DatagramSocket(0, InetAddress.getLoopbackAddress())), nullValue());
    }

    // Stable host id.

    @Test
    void getStableHostId_is_a_truncated_sha256_of_the_hostname() {
        assertThat(HostContext.getStableHostId(),
                equalTo(HostContext.computeStableHostId(HostContext.getHostname())));
    }

    @Test
    void getStableHostId_is_sixteen_hexadecimal_characters() {
        assertThat(HostContext.getStableHostId(), matchesPattern("[0-9a-f]{16}"));
    }

    @Test
    void computeStableHostId_matches_a_known_digest() throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final byte[] hash = digest.digest("example-host".getBytes(StandardCharsets.UTF_8));
        final StringBuilder expected = new StringBuilder();
        for (final byte b : hash) {
            expected.append(String.format("%02x", b));
        }

        assertThat(HostContext.computeStableHostId("example-host"),
                equalTo(expected.substring(0, 16)));
    }

    @Test
    void computeStableHostId_distinguishes_two_hostnames() {
        assertThat(HostContext.computeStableHostId("host-a"),
                not(equalTo(HostContext.computeStableHostId("host-b"))));
    }

    @Test
    void computeStableHostId_fails_when_the_digest_is_unavailable() {
        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> HostContext.computeStableHostId("any-host", () -> {
                    throw new NoSuchAlgorithmException("no SHA-256");
                }));

        assertThat(exception.getMessage(), equalTo("SHA-256 algorithm not available"));
    }

    private static java.util.List<String> localAddresses() throws SocketException {
        final java.util.List<String> addresses = new java.util.ArrayList<>();
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
