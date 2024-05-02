/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.UnmodifiableFuture;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class LogThrottlingStrategyTest {
    private static final int TEST_MAX_PENDING_REQUEST = 1;
    private BlockingQueue<Runnable> testQueue;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private HttpRequest httpRequest;

    private LogThrottlingStrategy objectUnderTest;

    @BeforeEach
    public void setUp() {
        testQueue = new LinkedBlockingQueue<>();
        objectUnderTest = new LogThrottlingStrategy(TEST_MAX_PENDING_REQUEST, testQueue);
    }

    @Test
    public void testNullWorkQueue() {
        assertThrows(NullPointerException.class, () -> new LogThrottlingStrategy(TEST_MAX_PENDING_REQUEST, null));
    }

    @Test
    public void testAcceptSuccess() {
        // When
        CompletionStage<Boolean> completionStage = objectUnderTest.accept(serviceRequestContext, httpRequest);

        // Then
        assertEquals(UnmodifiableFuture.completedFuture(true), completionStage);
    }

    @Test
    public void testAcceptFail() {
        // Prepare
        testQueue.add(() -> { });

        // When
        CompletionStage<Boolean> completionStage = objectUnderTest.accept(serviceRequestContext, httpRequest);

        // Then
        assertEquals(UnmodifiableFuture.completedFuture(false), completionStage);
    }
}