/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.host;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides the identity of the current Data Prepper instance.
 * This is intended as a shared utility so that host identity is consistent across all components
 * (processors, sinks, source coordinators, etc.).
 * <p>
 * Resolution falls back through several strategies because the first of them, resolving the local
 * hostname through the name service, fails in ordinary containers whose hostname has no entry in
 * {@code /etc/hosts} or DNS. Returning a single shared value in that case is worse than it appears:
 * components which use this as a per-instance discriminator, such as metric labels, silently collide
 * with every other instance rather than reporting an error. A name which resolves but distinguishes
 * nothing, such as {@code localhost}, counts as unresolved for the same reason.
 */
public class HostContext {

    private static final Logger LOG = LoggerFactory.getLogger(HostContext.class);

    static final String UNKNOWN_HOST = "unknown";
    static final String HOSTNAME_ENVIRONMENT_VARIABLE = "HOSTNAME";

    /** Values every instance would report, so resolution continues past them to a local address. */
    private static final Set<String> UNUSABLE_IDENTITIES = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("localhost", "localhost.localdomain", "127.0.0.1", "::1")));

    /**
     * RFC 5737 TEST-NET-1. Connecting a datagram socket performs a routing lookup without sending a
     * packet, so this address is never contacted; it only asks the operating system which local
     * address it would send from.
     */
    static final String ROUTE_LOOKUP_ADDRESS = "192.0.2.1";
    private static final int ROUTE_LOOKUP_PORT = 9;

    private static final int STABLE_HOST_ID_LENGTH = 16;
    private static final String DIGEST_ALGORITHM = "SHA-256";

    private static final String HOSTNAME = resolveHostname();
    private static final String STABLE_HOST_ID = computeStableHostId(HOSTNAME);

    static {
        // So an emitted identifier can be traced back to its host.
        LOG.info("Resolved the identity of this host as '{}', with stable host id {}.", HOSTNAME, STABLE_HOST_ID);
    }

    @FunctionalInterface
    interface LocalHostSupplier {
        InetAddress get() throws UnknownHostException;
    }

    @FunctionalInterface
    interface InterfaceSupplier {
        Enumeration<NetworkInterface> get() throws SocketException;
    }

    @FunctionalInterface
    interface SocketSupplier {
        DatagramSocket get() throws SocketException;
    }

    @FunctionalInterface
    interface DigestSupplier {
        MessageDigest get() throws NoSuchAlgorithmException;
    }

    /**
     * Returns the identity of the current Data Prepper host: the hostname where one resolves, and a
     * local interface address otherwise. Callers must not assume the value resolves as a name.
     *
     * @return the hostname or a local address, or {@code "unknown"} when nothing resolved
     */
    public static String getHostname() {
        return HOSTNAME;
    }

    /**
     * Returns a stable identifier for this host: a truncated SHA-256 hash of {@link #getHostname()}.
     * It keeps the hostname and address out of emitted data, but the hash is unsalted over a small
     * input space, so it obscures the identity rather than protecting it. When
     * {@link #isHostIdentityResolved()} is false it hashes the placeholder, identical on every instance.
     *
     * @return the host identifier, never null
     */
    public static String getStableHostId() {
        return STABLE_HOST_ID;
    }

    /**
     * Returns whether an identity could actually be resolved for this host. When this is false every
     * instance shares one value, so callers which depend on distinguishing instances should treat it
     * as a failure rather than proceeding.
     *
     * @return true when the host identity is a resolved value rather than a shared placeholder
     */
    public static boolean isHostIdentityResolved() {
        return isHostIdentityResolved(HOSTNAME);
    }

    static boolean isHostIdentityResolved(final String hostname) {
        return !UNKNOWN_HOST.equals(hostname);
    }

    static String resolveHostname() {
        return resolveHostname(HostContext::resolveFromLocalHost,
                HostContext::resolveFromEnvironment,
                HostContext::resolveFromNetworkInterfaces,
                HostContext::resolveFromRouteLookup);
    }

    static String resolveHostname(final Supplier<String> localHost,
                                  final Supplier<String> environment,
                                  final Supplier<String> networkInterfaces,
                                  final Supplier<String> routeLookup) {
        final String localHostname = localHost.get();
        if (localHostname != null) {
            return localHostname;
        }

        final String environmentHostname = environment.get();
        if (environmentHostname != null) {
            LOG.warn("Unable to resolve the local host; using the {} environment variable as the host identity.",
                    HOSTNAME_ENVIRONMENT_VARIABLE);
            return environmentHostname;
        }

        final String interfaceAddress = networkInterfaces.get();
        if (interfaceAddress != null) {
            LOG.warn("Unable to resolve a hostname; using a local interface address as the host identity.");
            return interfaceAddress;
        }

        final String routedAddress = routeLookup.get();
        if (routedAddress != null) {
            LOG.warn("Unable to resolve a hostname or read the network interfaces; using the routed " +
                    "source address as the host identity.");
            return routedAddress;
        }

        LOG.error("Unable to resolve any identity for this host; using '{}'. Components which rely on " +
                "a distinct host identity, such as per-instance metric labels, will collide with other " +
                "instances in this state.", UNKNOWN_HOST);
        return UNKNOWN_HOST;
    }

    static String resolveFromLocalHost() {
        return resolveFromLocalHost(InetAddress::getLocalHost);
    }

    static String resolveFromLocalHost(final LocalHostSupplier localHostSupplier) {
        try {
            return usableOrNull(localHostSupplier.get().getHostName());
        } catch (final Exception e) {
            LOG.debug("Unable to resolve the local host: {}", e.getMessage());
            return null;
        }
    }

    static String resolveFromEnvironment() {
        return resolveFromEnvironment(System::getenv);
    }

    static String resolveFromEnvironment(final Function<String, String> environment) {
        try {
            return usableOrNull(environment.apply(HOSTNAME_ENVIRONMENT_VARIABLE));
        } catch (final RuntimeException e) {
            // Resolution runs at class init, where a throw becomes ExceptionInInitializerError.
            LOG.debug("Unable to read the {} environment variable: {}", HOSTNAME_ENVIRONMENT_VARIABLE, e.getMessage());
            return null;
        }
    }

    static String resolveFromNetworkInterfaces() {
        return resolveFromNetworkInterfaces(NetworkInterface::getNetworkInterfaces);
    }

    static String resolveFromNetworkInterfaces(final InterfaceSupplier interfaceSupplier) {
        final List<String> candidates = new ArrayList<>();
        try {
            final Enumeration<NetworkInterface> interfaces = interfaceSupplier.get();
            while (interfaces != null && interfaces.hasMoreElements()) {
                collectAddresses(interfaces.nextElement(), candidates);
            }
        } catch (final SocketException e) {
            LOG.debug("Unable to read the local network interfaces: {}", e.getMessage());
            return null;
        }

        if (candidates.isEmpty()) {
            return null;
        }

        // Sorted so that a host with more than one address reports the same one on every restart.
        Collections.sort(candidates);
        return candidates.get(0);
    }

    private static void collectAddresses(final NetworkInterface networkInterface, final List<String> candidates) {
        try {
            if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                return;
            }
        } catch (final SocketException e) {
            LOG.debug("Unable to read the state of a network interface: {}", e.getMessage());
            return;
        }

        final Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
            final InetAddress address = addresses.nextElement();
            if (isUsableAddress(address)) {
                candidates.add(address.getHostAddress());
            }
        }
    }

    static String resolveFromRouteLookup() {
        return resolveFromRouteLookup(DatagramSocket::new);
    }

    static String resolveFromRouteLookup(final SocketSupplier socketSupplier) {
        try (DatagramSocket socket = socketSupplier.get()) {
            socket.connect(InetAddress.getByName(ROUTE_LOOKUP_ADDRESS), ROUTE_LOOKUP_PORT);
            final InetAddress local = socket.getLocalAddress();
            return isUsableAddress(local) ? local.getHostAddress() : null;
        } catch (final Exception e) {
            LOG.debug("Unable to determine a source address by routing lookup: {}", e.getMessage());
            return null;
        }
    }

    static String computeStableHostId(final String hostname) {
        return computeStableHostId(hostname, () -> MessageDigest.getInstance(DIGEST_ALGORITHM));
    }

    static String computeStableHostId(final String hostname, final DigestSupplier digestSupplier) {
        try {
            final byte[] hash = digestSupplier.get().digest(hostname.getBytes(StandardCharsets.UTF_8));
            return Hex.encodeHexString(hash).substring(0, STABLE_HOST_ID_LENGTH);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(DIGEST_ALGORITHM + " algorithm not available", e);
        }
    }

    private static boolean isUsableAddress(final InetAddress address) {
        return address != null
                && !address.isLoopbackAddress()
                && !address.isLinkLocalAddress()
                && !address.isAnyLocalAddress()
                && !address.isMulticastAddress();
    }

    private static String usableOrNull(final String hostname) {
        if (hostname == null) {
            return null;
        }
        final String trimmed = hostname.trim();
        if (trimmed.isEmpty() || UNUSABLE_IDENTITIES.contains(trimmed.toLowerCase(Locale.ROOT))) {
            return null;
        }
        return trimmed;
    }
}
