/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class IpAddressTest {
    @Test
    void ipAddress_returns_random_IP_address() {
        final IpAddress ipAddress = IpAddress.getInstance();

        assertThat(ipAddress, notNullValue());

        final String ipAddressString = ipAddress.ipAddress();

        assertThat(ipAddressString, notNullValue());
        assertThat(ipAddressString, not(emptyString()));
    }

    @Test
    void ipAddress_with_multiple_calls_returns_different_addresses() {
        final IpAddress ipAddress = IpAddress.getInstance();

        assertThat(ipAddress, notNullValue());

        final int requestCount = 200;
        final Set<String> results = new HashSet<>();
        for(int i = 0; i < requestCount; i++)
            results.add(ipAddress.ipAddress());

        assertThat(results.size(), greaterThanOrEqualTo(requestCount / 2));
    }
}
