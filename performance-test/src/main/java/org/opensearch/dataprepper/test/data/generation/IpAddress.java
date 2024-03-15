/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class IpAddress {
    private static final IpAddress IP_ADDRESS = new IpAddress();
    private static final String LOCALHOST = "127.0.0.1";
    private final Random random = new Random();
    private final List<IpAddressProvider> ipAddressProviders;

    private IpAddress() {
        ipAddressProviders = new ArrayList<>();
        getIpAddressProvider("ipv4", IpAddressGenerationOption.DEFINED)
                .ifPresent(ipAddressProviders::add);
        getIpAddressProvider("ipv6", IpAddressGenerationOption.OFF)
                .ifPresent(ipAddressProviders::add);
    }

    private Optional<IpAddressProvider> getIpAddressProvider(final String ipVersionIdentifier, final IpAddressGenerationOption defaultGenerationOption) {

        final String generationName = System.getProperty(ipVersionIdentifier, defaultGenerationOption.getValue());
        final IpAddressGenerationOption option = IpAddressGenerationOption.fromValue(generationName);

        switch (option) {
            case DEFINED:
                final InputStream inputStream = loadStream("/data/" + ipVersionIdentifier + "-addresses.txt");
                try {
                    return Optional.of(IpAddressProvider.fromInputStream(inputStream));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case GENERATED:
                if(ipVersionIdentifier.equals("ipv4"))
                    return Optional.of(IpAddressProvider.generatedIpV4(50_000));
                else
                    throw new RuntimeException("Generated is only supported for ipv4 at the current time.");
        }
        return Optional.empty();
    }

    public static IpAddress getInstance() {
        return IP_ADDRESS;
    }

    public String ipAddress() {
        if(ipAddressProviders.isEmpty())
            return LOCALHOST;
        final IpAddressProvider ipAddressProvider = randomFromList(ipAddressProviders);
        return ipAddressProvider.ipAddress();
    }

    private <T> T randomFromList(final List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    private static InputStream loadStream(final String file) {
        final InputStream resourceAsStream = IpAddress.class.getResourceAsStream(file);
        if(resourceAsStream == null)
            throw new RuntimeException("Unable to load stream: " + file);
        return resourceAsStream;
    }
}
