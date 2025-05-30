/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.utils.RdsSourceAggregateMetrics;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.stream.BinlogClientWrapper.ACCESS_DENIED;
import static org.opensearch.dataprepper.plugins.source.rds.stream.BinlogClientWrapper.CONNECTION_REFUSED;
import static org.opensearch.dataprepper.plugins.source.rds.stream.BinlogClientWrapper.FAILED_TO_DETERMINE_BINLOG_FILENAME;


@ExtendWith(MockitoExtension.class)
class BinlogClientWrapperTest {
    @Mock
    private BinaryLogClient binaryLogClient;

    @Mock
    private RdsSourceAggregateMetrics rdsSourceAggregateMetrics;

    @Mock
    private Counter streamApiInvocations;

    @Mock
    private Counter stream4xxErrors;

    @Mock
    private Counter stream5xxErrors;

    @Mock
    private Counter streamAuthErrors;

    @Mock
    private Counter streamServerNotFoundErrors;

    @Mock
    private Counter streamReplicationNotEnabledErrors;

    @Mock
    private Counter streamAccessDeniedErrors;

    private BinlogClientWrapper binlogClientWrapper;

    @BeforeEach
    void setUp() {
        binlogClientWrapper = createObjectUnderTest();
        lenient().when(rdsSourceAggregateMetrics.getStreamApiInvocations()).thenReturn(streamApiInvocations);
        lenient().when(rdsSourceAggregateMetrics.getStream4xxErrors()).thenReturn(stream4xxErrors);
        lenient().when(rdsSourceAggregateMetrics.getStream5xxErrors()).thenReturn(stream5xxErrors);
        lenient().when(rdsSourceAggregateMetrics.getStreamAuthErrors()).thenReturn(streamAuthErrors);
        lenient().when(rdsSourceAggregateMetrics.getStreamServerNotFoundErrors()).thenReturn(streamServerNotFoundErrors);
        lenient().when(rdsSourceAggregateMetrics.getStreamReplicationNotEnabledErrors()).thenReturn(streamReplicationNotEnabledErrors);
        lenient().when(rdsSourceAggregateMetrics.getStreamAccessDeniedErrors()).thenReturn(streamAccessDeniedErrors);
    }

    @Test
    void test_connect_calls_client_connect() throws IOException {
        binlogClientWrapper.connect();
        verify(binaryLogClient).connect();
        verify(streamApiInvocations).increment();
    }

    @Test
    void test_connect_with_4xx_auth_exception() throws IOException {
        doThrow(AuthenticationException.class).when(binaryLogClient).connect();

        try {
            binlogClientWrapper.connect();
        } catch (Exception e) {
            verify(streamApiInvocations).increment();
            verify(stream4xxErrors).increment();
            verify(streamAuthErrors).increment();
        }
    }

    @Test
    void test_connect_with_4xx_server_not_found_exception() throws IOException {
        Exception connectionRefusedException = new IOException(
                "Failed to connect to MySQL server",
                new Exception(CONNECTION_REFUSED)
        );
        doThrow(connectionRefusedException).when(binaryLogClient).connect();

        try {
            binlogClientWrapper.connect();
        } catch (Exception e) {
            verify(streamApiInvocations).increment();
            verify(stream4xxErrors).increment();
            verify(streamServerNotFoundErrors).increment();
        }
    }

    @Test
    void test_connect_with_4xx_binlog_exception() throws IOException {
        final Exception binlogNotEnabledException = new IOException(FAILED_TO_DETERMINE_BINLOG_FILENAME);
        doThrow(binlogNotEnabledException).when(binaryLogClient).connect();

        try {
            binlogClientWrapper.connect();
        } catch (Exception e) {
            verify(streamApiInvocations).increment();
            verify(stream4xxErrors).increment();
            verify(streamReplicationNotEnabledErrors).increment();
        }
    }

    @Test
    void test_connect_with_4xx_access_exception() throws IOException {
        final Exception accessDeniedException = new IOException(ACCESS_DENIED);
        doThrow(accessDeniedException).when(binaryLogClient).connect();

        try {
            binlogClientWrapper.connect();
        } catch (Exception e) {
            verify(streamApiInvocations).increment();
            verify(stream4xxErrors).increment();
            verify(streamAccessDeniedErrors).increment();
        }
    }

    @Test
    void test_connect_with_5xx_exception() throws IOException {
        doThrow(RuntimeException.class).when(binaryLogClient).connect();

        try {
            binlogClientWrapper.connect();
        } catch (Exception e) {
            verify(streamApiInvocations).increment();
            verify(stream5xxErrors).increment();
        }
    }

    @Test
    void test_disconnect_calls_client_disconnect() throws IOException {
        BinlogEventListener eventListener = mock(BinlogEventListener.class);
        when(binaryLogClient.getEventListeners()).thenReturn(List.of(eventListener));
        BinaryLogClient.LifecycleListener lifecycleListener = mock(BinaryLogClient.LifecycleListener.class);
        when(binaryLogClient.getLifecycleListeners()).thenReturn(List.of(lifecycleListener));

        binlogClientWrapper.disconnect();

        InOrder inOrder = inOrder(binaryLogClient, eventListener);
        inOrder.verify(eventListener).stopCheckpointManager();
        inOrder.verify(binaryLogClient).unregisterEventListener(eventListener);
        inOrder.verify(binaryLogClient).unregisterLifecycleListener(lifecycleListener);
        inOrder.verify(binaryLogClient).disconnect();
    }

    private BinlogClientWrapper createObjectUnderTest() {
        return new BinlogClientWrapper(binaryLogClient, rdsSourceAggregateMetrics);
    }
}
