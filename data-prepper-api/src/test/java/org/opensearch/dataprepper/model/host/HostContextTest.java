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
import org.mockito.MockedStatic;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.emptyString;
import static org.mockito.Mockito.mockStatic;

class HostContextTest {

    @Test
    void getHostname_returns_non_null_non_empty_value() {
        final String hostname = HostContext.getHostname();
        assertThat(hostname, notNullValue());
        assertThat(hostname, not(emptyString()));
    }

    @Test
    void getHostname_returns_consistent_value() {
        final String first = HostContext.getHostname();
        final String second = HostContext.getHostname();
        assertThat(first, equalTo(second));
    }

    @Test
    void getHostname_matches_InetAddress_hostname() throws UnknownHostException {
        final String expected = InetAddress.getLocalHost().getHostName();
        assertThat(HostContext.getHostname(), equalTo(expected));
    }

    @Test
    void resolveHostname_returns_valid_hostname() throws UnknownHostException {
        final String hostname = HostContext.resolveHostname();
        assertThat(hostname, equalTo(InetAddress.getLocalHost().getHostName()));
    }

    @Test
    void resolveHostname_returns_unknown_when_hostname_cannot_be_resolved() {
        try (final MockedStatic<InetAddress> inetAddressMock = mockStatic(InetAddress.class)) {
            inetAddressMock.when(InetAddress::getLocalHost)
                    .thenThrow(new UnknownHostException("test exception"));

            assertThat(HostContext.resolveHostname(), equalTo("unknown"));
        }
    }

    @Test
    void constructor_can_be_created() {
        final HostContext hostContext = new HostContext();
        assertThat(hostContext, notNullValue());
    }
}
