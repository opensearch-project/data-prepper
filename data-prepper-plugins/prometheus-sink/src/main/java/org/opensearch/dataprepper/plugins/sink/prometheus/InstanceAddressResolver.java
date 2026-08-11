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

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Resolves an address which distinguishes this Data Prepper instance from the others running the
 * same pipeline.
 * <p>
 * The hostname is deliberately not used. Hostname resolution depends on the hostname being present in
 * the name service, and when it is absent the usual result is a single fallback value shared by every
 * instance. A shared value is worse than no value here, because identical values are what produce the
 * colliding time series this label exists to separate.
 */
public final class InstanceAddressResolver {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceAddressResolver.class);

    private static final int DEFAULT_HTTPS_PORT = 443;
    private static final int DEFAULT_HTTP_PORT = 80;

    private InstanceAddressResolver() {
    }

    /**
     * Returns the IPv4 address of this instance, preferring the address which reaches the given endpoint.
     *
     * @param endpointUrl the URL this sink writes to
     * @return the address, never null and never a placeholder
     * @throws InvalidPluginConfigurationException if no address is available
     */
    public static String resolveLocalIpv4(final String endpointUrl) {
        final String routedAddress = resolveViaRouteTo(endpointUrl);
        if (routedAddress != null) {
            return routedAddress;
        }
        return resolveLocalIpv4(InstanceAddressResolver::localInterfaces);
    }

    /**
     * Returns the address the operating system would use as the source when writing to the endpoint, or
     * null when it cannot be determined.
     * <p>
     * A datagram socket is connected rather than a stream socket, so this performs a routing lookup
     * without sending a packet or opening a connection. On a host with more than one address this picks
     * the address the endpoint would observe, which is a better identity for the instance than an
     * arbitrary choice among the addresses assigned locally.
     */
    static String resolveViaRouteTo(final String endpointUrl) {
        final InetAddress endpoint;
        final int port;
        try {
            final URI uri = URI.create(endpointUrl);
            if (uri.getHost() == null) {
                return null;
            }
            endpoint = firstIpv4(InetAddress.getAllByName(uri.getHost()));
            port = uri.getPort() > 0 ? uri.getPort() : defaultPortFor(uri.getScheme());
        } catch (final Exception e) {
            LOG.debug("Unable to resolve the endpoint to choose a source address, reading the local " +
                    "interfaces instead: {}", e.getMessage());
            return null;
        }
        if (endpoint == null) {
            return null;
        }

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(endpoint, port);
            final InetAddress local = socket.getLocalAddress();
            if (isUsableAddress(local)) {
                return local.getHostAddress();
            }
        } catch (final Exception e) {
            LOG.debug("Unable to determine the source address for the endpoint, reading the local " +
                    "interfaces instead: {}", e.getMessage());
        }
        return null;
    }

    private static InetAddress firstIpv4(final InetAddress[] addresses) {
        for (final InetAddress address : addresses) {
            if (address instanceof Inet4Address) {
                return address;
            }
        }
        return null;
    }

    private static int defaultPortFor(final String scheme) {
        return "http".equalsIgnoreCase(scheme) ? DEFAULT_HTTP_PORT : DEFAULT_HTTPS_PORT;
    }

    private static boolean isUsableAddress(final InetAddress address) {
        return address instanceof Inet4Address
                && !address.isLoopbackAddress()
                && !address.isLinkLocalAddress()
                && !address.isAnyLocalAddress();
    }

    static String resolveLocalIpv4(final InterfaceSupplier interfaceSupplier) {
        final List<String> candidates = new ArrayList<>();
        final Enumeration<NetworkInterface> interfaces;
        try {
            interfaces = interfaceSupplier.get();
        } catch (final SocketException e) {
            throw new InvalidPluginConfigurationException(
                    "Unable to read the local network interfaces to resolve a value for instance_label.", e);
        }

        while (interfaces != null && interfaces.hasMoreElements()) {
            final NetworkInterface networkInterface = interfaces.nextElement();
            if (!isUsable(networkInterface)) {
                continue;
            }
            final Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                final InetAddress address = addresses.nextElement();
                if (isUsableAddress(address)) {
                    candidates.add(address.getHostAddress());
                }
            }
        }

        if (candidates.isEmpty()) {
            throw new InvalidPluginConfigurationException(
                    "Unable to resolve an IPv4 address for instance_label. No non-loopback IPv4 address is " +
                            "assigned to any local network interface, so this sink cannot produce a value which " +
                            "distinguishes this instance from the others writing to the same endpoint. Give this " +
                            "instance a routable address, or remove instance_label to write series without it.");
        }

        // Only reached when the routing lookup gave no answer, so there is nothing to distinguish the
        // candidates by. Sorted so that an instance reports the same one on every restart.
        Collections.sort(candidates);
        return candidates.get(0);
    }

    private static boolean isUsable(final NetworkInterface networkInterface) {
        try {
            return networkInterface.isUp() && !networkInterface.isLoopback();
        } catch (final SocketException e) {
            return false;
        }
    }

    private static Enumeration<NetworkInterface> localInterfaces() throws SocketException {
        return NetworkInterface.getNetworkInterfaces();
    }

    @FunctionalInterface
    interface InterfaceSupplier {
        Enumeration<NetworkInterface> get() throws SocketException;
    }
}
