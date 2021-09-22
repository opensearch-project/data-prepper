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

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogHTTPSourceTest {

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;

    private PluginSetting testPluginSetting;
    private BlockingBuffer<Record<String>> testBuffer;
    private LogHTTPSource logHTTPSourceUnderTest;

    private BlockingBuffer<Record<String>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }

    @BeforeEach
    public void setUp() {
        lenient().when(serverBuilder.annotatedService(any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        testPluginSetting = new PluginSetting("log_http_source", new HashMap<>());
        testPluginSetting.setPipelineName("pipeline");
        testBuffer = getBuffer();
        logHTTPSourceUnderTest = new LogHTTPSource(testPluginSetting);
    }

    @AfterEach
    public void cleanUp() {
        logHTTPSourceUnderTest.stop();
    }

    @Test
    public void testHTTPJsonResponse() {
        logHTTPSourceUnderTest.start(testBuffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.GET)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertThat(i.status().code()).isEqualTo(200)).join();
        assertFalse(testBuffer.isEmpty());
    }

    @Test
    public void testDoubleStart() {
        // starting server
        logHTTPSourceUnderTest.start(testBuffer);
        // double start server
        Assertions.assertThrows(IllegalStateException.class, () -> logHTTPSourceUnderTest.start(testBuffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.emptyMap());
        testPluginSetting.setPipelineName("pipeline");
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(testBuffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, source::stop);
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final LogHTTPSource source = new LogHTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, source::stop);
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        logHTTPSourceUnderTest.start(testBuffer);

        final LogHTTPSource secondSource = new LogHTTPSource(testPluginSetting);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> secondSource.start(testBuffer));
    }

    // TODO: write test cases
    public static void main(String[] args) {
        LogHTTPSource logHTTPSource = new LogHTTPSource(new PluginSetting("log_http_source", new HashMap<>()));
        Buffer<Record<String>> blockingBuffer = new BlockingBuffer<Record<String>>(10, 8, "test-pipeline");
        logHTTPSource.start(blockingBuffer);
    }
}