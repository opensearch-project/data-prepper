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
import java.util.Arrays;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
    void getHostIdentity_returns_non_null_non_empty_value() {
        final String identity = HostContext.getHostIdentity();
        assertThat(identity, notNullValue());
        assertThat(identity, not(emptyString()));
    }

    @Test
    void getHostIdentity_returns_consistent_value() {
        assertThat(HostContext.getHostIdentity(), equalTo(HostContext.getHostIdentity()));
    }

    @Test
    void isHostIdentityResolved_reports_whether_the_identity_is_the_placeholder() {
        assertThat(HostContext.isHostIdentityResolved(),
                equalTo(!"unknown".equals(HostContext.getHostIdentity())));
    }

    @Test
    void isHostIdentityResolved_is_false_only_for_the_unknown_placeholder() {
        assertThat(HostContext.isHostIdentityResolved(HostContext.UNKNOWN_HOST), is(false));
        assertThat(HostContext.isHostIdentityResolved("some-host"), is(true));
        assertThat(HostContext.isHostIdentityResolved("10.1.2.3"), is(true));
    }

    @Test
    void constructor_can_be_created() {
        assertThat(new HostContext(), notNullValue());
    }

    // The hostname, which reports only what the name service resolves.

    @Test
    void resolveHostname_returns_the_name_of_the_local_host() throws UnknownHostException {
        final InetAddress named = InetAddress.getByAddress("host-a", new byte[]{10, 1, 2, 3});
        assertThat(HostContext.resolveHostname(() -> named), equalTo("host-a"));
    }

    @Test
    void resolveHostname_returns_a_name_which_does_not_distinguish_the_host() throws UnknownHostException {
        // Filtering belongs to the identity, so the hostname reports whatever the name service gave.
        final InetAddress named = InetAddress.getByAddress("localhost", new byte[]{127, 0, 0, 1});
        assertThat(HostContext.resolveHostname(() -> named), equalTo("localhost"));
    }

    @Test
    void resolveHostname_returns_unknown_when_the_lookup_fails() {
        assertThat(HostContext.resolveHostname(() -> {
            throw new UnknownHostException("no local host");
        }), equalTo(HostContext.UNKNOWN_HOST));
    }

    @Test
    void resolveHostname_reads_the_real_name_service_without_failing() {
        // What it answers depends on the machine, so assert only that it runs and is stable.
        assertThat(HostContext.resolveHostname(), equalTo(HostContext.resolveHostname()));
    }

    // The identity, and the fallback chain behind it.

    @Test
    void resolveHostIdentity_prefers_the_hostname() {
        assertThat(HostContext.resolveHostIdentity("host-a"), equalTo("host-a"));
    }

    @Test
    void resolveHostIdentity_falls_past_a_hostname_which_distinguishes_nothing() {
        assertThat(HostContext.resolveHostIdentity("localhost"), not(equalTo("localhost")));
    }

    @Test
    void resolveHostIdentity_falls_past_the_unresolved_placeholder() {
        // Both are skipped, so they reach the same later strategy on this machine.
        assertThat(HostContext.resolveHostIdentity(HostContext.UNKNOWN_HOST),
                equalTo(HostContext.resolveHostIdentity("localhost")));
    }

    @Test
    void resolveHostIdentity_prefers_the_local_host() {
        assertThat(HostContext.resolveHostIdentity(() -> "from-local-host", () -> "from-env",
                () -> "from-interfaces", () -> "from-route"), equalTo("from-local-host"));
    }

    @Test
    void resolveHostIdentity_falls_back_to_the_environment() {
        assertThat(HostContext.resolveHostIdentity(() -> null, () -> "from-env",
                () -> "from-interfaces", () -> "from-route"), equalTo("from-env"));
    }

    @Test
    void resolveHostIdentity_falls_back_to_the_network_interfaces() {
        assertThat(HostContext.resolveHostIdentity(() -> null, () -> null,
                () -> "from-interfaces", () -> "from-route"), equalTo("from-interfaces"));
    }

    @Test
    void resolveHostIdentity_falls_back_to_the_routing_lookup() {
        assertThat(HostContext.resolveHostIdentity(() -> null, () -> null,
                () -> null, () -> "from-route"), equalTo("from-route"));
    }

    @Test
    void resolveHostIdentity_returns_unknown_when_every_strategy_fails() {
        assertThat(HostContext.resolveHostIdentity(() -> null, () -> null, () -> null, () -> null),
                equalTo(HostContext.UNKNOWN_HOST));
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

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "LocalHost", "localhost.localdomain", "127.0.0.1", "::1",
            " localhost ", "unknown"})
    void resolveFromEnvironment_rejects_a_value_which_does_not_distinguish_the_host(final String value) {
        assertThat(HostContext.resolveFromEnvironment(name -> value), nullValue());
    }

    @Test
    void resolveFromEnvironment_returns_null_when_the_variable_is_absent() {
        assertThat(HostContext.resolveFromEnvironment(name -> null), nullValue());
    }

    @Test
    void resolveFromEnvironment_returns_null_when_the_variable_cannot_be_read() {
        assertThat(HostContext.resolveFromEnvironment(name -> {
            throw new SecurityException("environment access denied");
        }), nullValue());
    }

    @Test
    void resolveFromEnvironment_reads_the_real_environment_without_failing() {
        assertThat(HostContext.resolveFromEnvironment(), equalTo(HostContext.resolveFromEnvironment()));
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
    void resolveFromNetworkInterfaces_returns_an_address_assigned_to_an_interface() throws Exception {
        final NetworkInterface networkInterface = usableInterface(InetAddress.getByName("10.1.2.30"));

        assertThat(HostContext.resolveFromNetworkInterfaces(() -> enumerationOf(networkInterface)),
                equalTo("10.1.2.30"));
    }

    @Test
    void resolveFromNetworkInterfaces_chooses_the_lowest_address_so_that_a_restart_reports_the_same_one()
            throws Exception {
        final NetworkInterface first = usableInterface(InetAddress.getByName("10.1.2.30"));
        final NetworkInterface second = usableInterface(InetAddress.getByName("10.1.2.20"));

        assertThat(HostContext.resolveFromNetworkInterfaces(() -> enumerationOf(first, second)),
                equalTo("10.1.2.20"));
        assertThat(HostContext.resolveFromNetworkInterfaces(() -> enumerationOf(second, first)),
                equalTo("10.1.2.20"));
    }

    @Test
    void resolveFromNetworkInterfaces_ignores_addresses_which_cannot_distinguish_a_host() throws Exception {
        final NetworkInterface networkInterface = usableInterface(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("169.254.1.1"),
                InetAddress.getByName("0.0.0.0"),
                InetAddress.getByName("224.0.0.1"));

        assertThat(HostContext.resolveFromNetworkInterfaces(() -> enumerationOf(networkInterface)), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_ignores_an_interface_which_is_down() throws Exception {
        final NetworkInterface down = mock(NetworkInterface.class);
        when(down.isUp()).thenReturn(false);

        assertThat(HostContext.resolveFromNetworkInterfaces(() -> enumerationOf(down)), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_ignores_the_loopback_interface() throws Exception {
        final NetworkInterface loopback = mock(NetworkInterface.class);
        when(loopback.isUp()).thenReturn(true);
        when(loopback.isLoopback()).thenReturn(true);

        assertThat(HostContext.resolveFromNetworkInterfaces(() -> enumerationOf(loopback)), nullValue());
    }

    @Test
    void resolveFromNetworkInterfaces_walks_the_real_interfaces_without_failing() {
        // What the interfaces hold depends on the machine, so assert only that it runs and is stable.
        assertThat(HostContext.resolveFromNetworkInterfaces(),
                equalTo(HostContext.resolveFromNetworkInterfaces()));
    }

    // Routing lookup.

    @Test
    void resolveFromRouteLookup_returns_the_source_address_the_socket_would_send_from() throws Exception {
        final DatagramSocket socket = mock(DatagramSocket.class);
        when(socket.getLocalAddress()).thenReturn(InetAddress.getByName("10.1.2.30"));

        assertThat(HostContext.resolveFromRouteLookup(() -> socket), equalTo("10.1.2.30"));
    }

    @Test
    void resolveFromRouteLookup_asks_for_the_route_to_the_documentation_address() throws Exception {
        final DatagramSocket socket = mock(DatagramSocket.class);
        when(socket.getLocalAddress()).thenReturn(InetAddress.getByName("10.1.2.30"));

        HostContext.resolveFromRouteLookup(() -> socket);

        // Never contacted, so it must stay an address which cannot belong to a real endpoint.
        verify(socket).connect(InetAddress.getByName(HostContext.ROUTE_LOOKUP_ADDRESS), 9);
    }

    @Test
    void resolveFromRouteLookup_performs_a_real_lookup_without_failing() {
        // Whether a route exists depends on the machine, so assert only that it runs and is stable.
        assertThat(HostContext.resolveFromRouteLookup(), equalTo(HostContext.resolveFromRouteLookup()));
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
    void getStableHostId_is_a_truncated_sha256_of_the_host_identity() throws NoSuchAlgorithmException {
        assertThat(HostContext.getStableHostId(), equalTo(sha256Prefix(HostContext.getHostIdentity())));
    }

    @Test
    void getStableHostId_is_sixteen_hexadecimal_characters() {
        assertThat(HostContext.getStableHostId(), matchesPattern("[0-9a-f]{16}"));
    }

    @Test
    void computeStableHostId_matches_a_known_digest() {
        assertThat(HostContext.computeStableHostId("example-host"), equalTo("8f1f0466fc41bb15"));
    }

    @Test
    void computeStableHostId_matches_the_digest_of_the_unresolved_placeholder() {
        assertThat(HostContext.computeStableHostId(HostContext.UNKNOWN_HOST), equalTo("b23a6a8439c0dde5"));
    }

    @Test
    void computeStableHostId_distinguishes_two_identities() {
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

    private static NetworkInterface usableInterface(final InetAddress... addresses) throws SocketException {
        final NetworkInterface networkInterface = mock(NetworkInterface.class);
        when(networkInterface.isUp()).thenReturn(true);
        when(networkInterface.isLoopback()).thenReturn(false);
        // A fresh enumeration per call, so an interface can be walked more than once.
        when(networkInterface.getInetAddresses())
                .thenAnswer(invocation -> Collections.enumeration(Arrays.asList(addresses)));
        return networkInterface;
    }

    private static Enumeration<NetworkInterface> enumerationOf(final NetworkInterface... interfaces) {
        return Collections.enumeration(Arrays.asList(interfaces));
    }

    private static String sha256Prefix(final String value) throws NoSuchAlgorithmException {
        final byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        final StringBuilder hex = new StringBuilder();
        for (final byte b : hash) {
            hex.append(String.format("%02x", b));
        }
        return hex.substring(0, 16);
    }
}
