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

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HTTPSourceTest {
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;

    private PluginSetting testPluginSetting;
    private BlockingBuffer<Record<String>> testBuffer;
    private HTTPSource HTTPSourceUnderTest;

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
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        testPluginSetting = new PluginSetting("http", new HashMap<>());
        testPluginSetting.setPipelineName("pipeline");
        testBuffer = getBuffer();
        HTTPSourceUnderTest = new HTTPSource(testPluginSetting);
    }

    @AfterEach
    public void cleanUp() {
        HTTPSourceUnderTest.stop();
    }

    @Test
    public void testHTTPJsonResponse200() {
        HTTPSourceUnderTest.start(testBuffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertThat(i.status()).isEqualTo(HttpStatus.OK)).join();
        Assertions.assertFalse(testBuffer.isEmpty());
    }

    @Test
    public void testHTTPJsonResponse429() throws InterruptedException {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int clientTimeoutInMillis = 100;
        final int serverTimeoutInMillis = (testMaxPendingRequests + testThreadCount + 1) * clientTimeoutInMillis;
        settings.put(HTTPSourceConfig.REQUEST_TIMEOUT, serverTimeoutInMillis);
        settings.put(HTTPSourceConfig.MAX_PENDING_REQUESTS, testMaxPendingRequests);
        settings.put(HTTPSourceConfig.THREAD_COUNT, testThreadCount);
        testPluginSetting = new PluginSetting("http", settings);
        testPluginSetting.setPipelineName("pipeline");
        HTTPSourceUnderTest = new HTTPSource(testPluginSetting);
        // Start the source
        HTTPSourceUnderTest.start(testBuffer);
        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:2021")
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8("[{\"log\": \"somelog\"}]");

        // Fill in the buffer
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate().whenComplete(
                (response, ex) -> assertThat(response.status()).isEqualTo(HttpStatus.OK)).join();

        // Send requests to throttle the server when buffer is full
        // Set the client timeout to be less than source serverTimeoutInMillis / (testMaxPendingRequests + testThreadCount)
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(clientTimeoutInMillis).build();
        for (int i = 0; i < testMaxPendingRequests + testThreadCount; i++) {
            CompletionException actualException = Assertions.assertThrows(
                    CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
            assertThat(actualException.getCause()).isInstanceOf(ResponseTimeoutException.class);
        }

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData).aggregate().whenComplete(
                (response, ex) -> assertThat(response.status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS)).join();
        // Wait until source server timeout a request processing thread
        Thread.sleep(serverTimeoutInMillis);
        // New request should timeout instead of being rejected
        CompletionException actualException = Assertions.assertThrows(
                CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
        assertThat(actualException.getCause()).isInstanceOf(ResponseTimeoutException.class);
    }

    @Test
    public void testServerStartCertFileSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);

            final Path certFilePath = Path.of(TEST_SSL_CERTIFICATE_FILE);
            final Path keyFilePath = Path.of(TEST_SSL_KEY_FILE);
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(HTTPSourceConfig.SSL, true);
            settingsMap.put(HTTPSourceConfig.SSL_CERTIFICATE_FILE, TEST_SSL_CERTIFICATE_FILE);
            settingsMap.put(HTTPSourceConfig.SSL_KEY_FILE, TEST_SSL_KEY_FILE);

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            HTTPSourceUnderTest = new HTTPSource(testPluginSetting);
            HTTPSourceUnderTest.start(testBuffer);
            HTTPSourceUnderTest.stop();

            final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
            final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
            verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
            final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
            final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
            assertThat(actualCertificate).isEqualTo(certAsString);
            assertThat(actualPrivateKey).isEqualTo(keyAsString);
        }
    }

    @Test
    void testHTTPSJsonResponse() {

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(HTTPSourceConfig.REQUEST_TIMEOUT, 200);
        settingsMap.put(HTTPSourceConfig.SSL, true);
        settingsMap.put(HTTPSourceConfig.SSL_CERTIFICATE_FILE, TEST_SSL_CERTIFICATE_FILE);
        settingsMap.put(HTTPSourceConfig.SSL_KEY_FILE, TEST_SSL_KEY_FILE);
        testPluginSetting = new PluginSetting("http", settingsMap);
        testPluginSetting.setPipelineName("pipeline");
        HTTPSourceUnderTest = new HTTPSource(testPluginSetting);

        testBuffer = getBuffer();
        HTTPSourceUnderTest.start(testBuffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertThat(i.status().code()).isEqualTo(200)).join();
    }

    @Test
    public void testDoubleStart() {
        // starting server
        HTTPSourceUnderTest.start(testBuffer);
        // double start server
        Assertions.assertThrows(IllegalStateException.class, () -> HTTPSourceUnderTest.start(testBuffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.emptyMap());
        testPluginSetting.setPipelineName("pipeline");
        final HTTPSource source = new HTTPSource(testPluginSetting);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            Assertions.assertThrows(RuntimeException.class, () -> source.start(testBuffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            Assertions.assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            Assertions.assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            Assertions.assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = Assertions.assertThrows(RuntimeException.class, source::stop);
            Assertions.assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(testPluginSetting);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            Assertions.assertThrows(RuntimeException.class, source::stop);
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        HTTPSourceUnderTest.start(testBuffer);

        final HTTPSource secondSource = new HTTPSource(testPluginSetting);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> secondSource.start(testBuffer));
    }
}