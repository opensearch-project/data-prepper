/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class IpAddress {
    private static final IpAddress IP_ADDRESS = new IpAddress();
    private static final String LOCALHOST = "127.0.0.1";
    private final Random random = new Random();
    private final List<List<String>> usableIpAddresses;

    private IpAddress() {
        final List<String> ipv4Addresses = loadFromFile("ipv4-addresses.txt", System.getProperty("ipv4", "true"));
        final List<String> ipv6Addresses = loadFromFile("ipv6-addresses.txt", System.getProperty("ipv6", "false"));

        usableIpAddresses = new ArrayList<>();
        if(!ipv4Addresses.isEmpty())
            usableIpAddresses.add(ipv4Addresses);
        if(!ipv6Addresses.isEmpty())
            usableIpAddresses.add(ipv6Addresses);
    }

    public static IpAddress getInstance() {
        return IP_ADDRESS;
    }

    public String ipAddress() {
        if(usableIpAddresses.isEmpty())
            return LOCALHOST;
        final List<String> addresses = randomFromList(usableIpAddresses);
        return randomFromList(addresses);
    }

    private <T> T randomFromList(final List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    private static List<String> loadFromFile(final String file, final String useBoolean) {
        if(!Boolean.parseBoolean(useBoolean))
            return Collections.emptyList();

        final List<String> ipAddresses = new ArrayList<>();
        try(final InputStream ipInputStream = IpAddress.class.getResourceAsStream("/data/" + file)) {
            if(ipInputStream == null)
                throw new RuntimeException("Unable to load IP address file " + file);
            final BufferedReader reader = new BufferedReader(new InputStreamReader(ipInputStream));

            while(reader.ready()) {
                final String ipAddress = reader.readLine();
                ipAddresses.add(ipAddress);
            }

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        return Collections.unmodifiableList(ipAddresses);
    }
}
