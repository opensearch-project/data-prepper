/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class IpAddressProvider {

    private static final Random RANDOM = new Random();
    private final List<String> ipAddresses;

    private IpAddressProvider(final List<String> ipAddresses) {
        this.ipAddresses = ipAddresses;
    }

    String ipAddress() {
        return ipAddresses.get(RANDOM.nextInt(ipAddresses.size()));
    }

    static IpAddressProvider generatedIpV4(final int numberOfAddresses) {
        return new IpAddressProvider(
                IntStream.range(0, numberOfAddresses)
                        .mapToObj(i -> randomIpAddress())
                        .collect(Collectors.toList())
        );
    }

    static IpAddressProvider fromInputStream(final InputStream inputStream) throws IOException {
        return new IpAddressProvider(loadFromStream(inputStream));
    }

    private static List<String> loadFromStream(final InputStream inputStream) throws IOException {
        final List<String> ipAddresses = new ArrayList<>();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        while(reader.ready()) {
            final String ipAddress = reader.readLine();
            ipAddresses.add(ipAddress);
        }

        return Collections.unmodifiableList(ipAddresses);
    }

    private static String randomIpAddress() {
        final StringBuilder ipBuilder = new StringBuilder(4 * 4);
        ipBuilder.append(RANDOM.nextInt(256));
        ipBuilder.append(".");
        ipBuilder.append(RANDOM.nextInt(256));
        ipBuilder.append(".");
        ipBuilder.append(RANDOM.nextInt(256));
        ipBuilder.append(".");
        ipBuilder.append(RANDOM.nextInt(256));
        return ipBuilder.toString();
    }
}
