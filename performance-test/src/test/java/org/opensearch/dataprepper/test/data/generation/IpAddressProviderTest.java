/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import com.google.common.net.InetAddresses;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class IpAddressProviderTest {
    @ParameterizedTest
    @ArgumentsSource(IpAddressFileArgumentsProvider.class)
    void fromInputStream_ipAddress_returns_non_empty_string(final String file) throws IOException {
        final InputStream resourceAsStream = IpAddress.class.getResourceAsStream("/data/" + file);

        final IpAddressProvider objectUnderTest = IpAddressProvider.fromInputStream(resourceAsStream);

        final String ipAddressString = objectUnderTest.ipAddress();

        assertThat(ipAddressString, notNullValue());
        assertThat(ipAddressString, not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(IpAddressFileArgumentsProvider.class)
    void fromInputStream_ipAddress_with_multiple_calls_returns_different_addresses(final String file) throws IOException {
        final InputStream resourceAsStream = IpAddress.class.getResourceAsStream("/data/" + file);

        final IpAddressProvider objectUnderTest = IpAddressProvider.fromInputStream(resourceAsStream);

        final int requestCount = 200;
        final Set<String> results = new HashSet<>();
        for(int i = 0; i < requestCount; i++)
            results.add(objectUnderTest.ipAddress());

        assertThat(results.size(), greaterThanOrEqualTo(requestCount / 2));
    }

    static class IpAddressFileArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments("ipv4-addresses.txt"),
                    arguments("ipv6-addresses.txt")
            );
        }
    }

    @RepeatedTest(20)
    void generatedIpV4_ipAddress_is_parseable_as_ip_address() {
        final String ipAddress = IpAddressProvider.generatedIpV4(100).ipAddress();
        assertThat(ipAddress, notNullValue());

        InetAddresses.forString(ipAddress);
    }

    @RepeatedTest(10)
    void generatedIpV4_ipAddress_with_one_returns_same_value() {
        final String ipAddress = IpAddressProvider.generatedIpV4(1).ipAddress();

        for (int i = 0; i < 100; i++)
            assertThat(ipAddress, equalTo(ipAddress));
    }

    @RepeatedTest(2)
    void generatedIpV4_ipAddress_with_multiple_calls_returns_different_addresses() {
        final IpAddressProvider objectUnderTest = IpAddressProvider.generatedIpV4(500);

        final int requestCount = 200;
        final Set<String> results = new HashSet<>();
        for(int i = 0; i < requestCount; i++)
            results.add(objectUnderTest.ipAddress());

        assertThat(results.size(), greaterThanOrEqualTo(requestCount / 2));
    }
}