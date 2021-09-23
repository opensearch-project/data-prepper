/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

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

@ExtendWith(MockitoExtension.class)
class LogThrottlingStrategyTest {
    private static final int TEST_MAX_PENDING_REQUEST = 1;
    private BlockingQueue<Runnable> testQueue;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private HttpRequest httpRequest;

    private LogThrottlingStrategy objUnderTest;

    @BeforeEach
    public void setUp() {
        testQueue = new LinkedBlockingQueue<>();
        objUnderTest = new LogThrottlingStrategy(TEST_MAX_PENDING_REQUEST, testQueue);
    }

    @Test
    public void testAcceptSuccess() {
        // When
        CompletionStage<Boolean> completionStage = objUnderTest.accept(serviceRequestContext, httpRequest);

        // Then
        assertEquals(UnmodifiableFuture.completedFuture(true), completionStage);
    }

    @Test
    public void testAcceptFail() {
        // Prepare
        testQueue.add(() -> { });

        // When
        CompletionStage<Boolean> completionStage = objUnderTest.accept(serviceRequestContext, httpRequest);

        // Then
        assertEquals(UnmodifiableFuture.completedFuture(false), completionStage);
    }
}