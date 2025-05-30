/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.utils;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.utils.ServerIdGenerator.MAX_SERVER_ID;
import static org.opensearch.dataprepper.plugins.source.rds.utils.ServerIdGenerator.MIN_SERVER_ID;

class ServerIdGeneratorTest {
    @Test
    public void generateServerId_shouldReturnValueWithinValidRange() {
        // When
        int serverId = ServerIdGenerator.generateServerId();

        // Then
        assertThat("Server ID should be within valid range",
                serverId, allOf(
                        greaterThanOrEqualTo(MIN_SERVER_ID),
                        lessThanOrEqualTo(MAX_SERVER_ID)
                ));
    }

    @Test
    public void generateServerId_shouldFallbackToRandomWhenInetAddressFails() {
        // Given - Mock InetAddress to throw exception
        try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class)) {
            mockedInetAddress.when(InetAddress::getLocalHost)
                    .thenThrow(new UnknownHostException("Mocked exception"));

            // When
            int serverId = ServerIdGenerator.generateServerId();

            // Then
            assertThat("Fallback should generate valid server ID",
                    serverId, allOf(
                            greaterThanOrEqualTo(MIN_SERVER_ID),
                            lessThanOrEqualTo(MAX_SERVER_ID)
                    ));

            // Verify the exception path was taken
            mockedInetAddress.verify(InetAddress::getLocalHost);
        }
    }

    @Test
    public void generateServerId_shouldHandleProcessHandleFailure() {
        // Given - Mock ProcessHandle to throw exception
        try (MockedStatic<ProcessHandle> mockedProcessHandle = mockStatic(ProcessHandle.class)) {
            mockedProcessHandle.when(ProcessHandle::current)
                    .thenThrow(new RuntimeException("Mocked process failure"));

            // When
            int serverId = ServerIdGenerator.generateServerId();

            // Then
            assertThat("Should fallback gracefully on ProcessHandle failure",
                    serverId, allOf(
                            greaterThanOrEqualTo(MIN_SERVER_ID),
                            lessThanOrEqualTo(MAX_SERVER_ID)
                    ));
        }
    }

    @Test
    public void generateServerId_shouldProduceDifferentValuesForDifferentHosts() {
        // Given - Mock different IP addresses
        try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class)) {

            // Mock first host
            InetAddress mockAddress1 = mock(InetAddress.class);
            when(mockAddress1.getHostAddress()).thenReturn("192.168.1.100");
            mockedInetAddress.when(InetAddress::getLocalHost).thenReturn(mockAddress1);

            int serverId1 = ServerIdGenerator.generateServerId();

            // Mock second host
            InetAddress mockAddress2 = mock(InetAddress.class);
            when(mockAddress2.getHostAddress()).thenReturn("192.168.1.200");
            mockedInetAddress.when(InetAddress::getLocalHost).thenReturn(mockAddress2);

            int serverId2 = ServerIdGenerator.generateServerId();

            // Then
            assertThat("Both IDs should be in valid range",
                    serverId1, allOf(
                            greaterThanOrEqualTo(MIN_SERVER_ID),
                            lessThanOrEqualTo(MAX_SERVER_ID)
                    ));

            assertThat("Both IDs should be in valid range",
                    serverId2, allOf(
                            greaterThanOrEqualTo(MIN_SERVER_ID),
                            lessThanOrEqualTo(MAX_SERVER_ID)
                    ));

            assertThat("Different hosts should likely generate different server IDs",
                    serverId1, is(not(equalTo(serverId2))));
        }
    }
}
