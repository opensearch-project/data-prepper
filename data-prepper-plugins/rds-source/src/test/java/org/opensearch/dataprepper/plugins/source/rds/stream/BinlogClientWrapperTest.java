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
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.utils.RdsSourceAggregateMetrics;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


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

    private BinlogClientWrapper binlogClientWrapper;

    @BeforeEach
    void setUp() {
        binlogClientWrapper = createObjectUnderTest();
        lenient().when(rdsSourceAggregateMetrics.getStreamApiInvocations()).thenReturn(streamApiInvocations);
        lenient().when(rdsSourceAggregateMetrics.getStream4xxErrors()).thenReturn(stream4xxErrors);
        lenient().when(rdsSourceAggregateMetrics.getStream5xxErrors()).thenReturn(stream5xxErrors);
    }

    @Test
    void test_connect_calls_client_connect() throws IOException {
        binlogClientWrapper.connect();
        verify(binaryLogClient).connect();
        verify(streamApiInvocations).increment();
    }

    @Test
    void test_connect_with_4xx_exception() throws IOException {
        doThrow(AuthenticationException.class).when(binaryLogClient).connect();

        try {
            binlogClientWrapper.connect();
        } catch (Exception e) {
            verify(streamApiInvocations).increment();
            verify(stream4xxErrors).increment();
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
