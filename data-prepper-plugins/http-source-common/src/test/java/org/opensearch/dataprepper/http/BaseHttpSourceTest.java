package org.opensearch.dataprepper.http;

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
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BaseHttpSourceTest {
    private final String PLUGIN_NAME = "opensearch_api";
    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private final int DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
    private final int DEFAULT_THREAD_COUNT = 200;
    private final int MAX_CONNECTIONS_COUNT = 500;
    private final int MAX_PENDING_REQUESTS_COUNT = 1024;
    private final String sourceName = "basic-http-api-source";

    private final String TEST_SSL_CERTIFICATE_FILE =
            Objects.requireNonNull(getClass().getClassLoader().getResource("test_cert.crt")).getFile();
    private final String TEST_SSL_KEY_FILE =
            Objects.requireNonNull(getClass().getClassLoader().getResource("test_decrypted_key.key")).getFile();

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;

    @Mock
    private BlockingBuffer<Record<Event>> testBuffer;

    @Mock
    private BaseHttpService BaseHttpService;

    private BaseHttpSource<Record<Event>> httpApiSource;
    private HttpServerConfig sourceConfig;
    private PluginMetrics pluginMetrics;
    private PluginFactory pluginFactory;
    private PipelineDescription pipelineDescription;

    private BaseHttpSource<Record<Event>> createObjectUnderTest() {
        return new BaseHttpSource<>(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription, sourceName, LoggerFactory.getLogger(BaseHttpService.class)) {
            @Override
            public BaseHttpService getHttpService(int bufferTimeoutInMillis, Buffer<Record<Event>> buffer, PluginMetrics pluginMetrics) {
                return BaseHttpService;
            }
        };
    }

    @BeforeEach
    public void setUp() {
        lenient().when(serverBuilder.annotatedService(any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        sourceConfig = mock(HttpServerConfig.class);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        lenient().when(sourceConfig.getPath()).thenReturn("/path");
        lenient().when(sourceConfig.getPort()).thenReturn(9092);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(DEFAULT_THREAD_COUNT);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(MAX_CONNECTIONS_COUNT);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(MAX_PENDING_REQUESTS_COUNT);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        pluginFactory = mock(PluginFactory.class);
        final ArmeriaHttpAuthenticationProvider authenticationProvider = mock(ArmeriaHttpAuthenticationProvider.class);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

        httpApiSource = createObjectUnderTest();
    }

    @AfterEach
    public void cleanUp() {
        if (httpApiSource != null) {
            httpApiSource.stop();
        }
    }

    @Test
    public void testServerStartCertFileSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);

            final Path certFilePath = new File(TEST_SSL_CERTIFICATE_FILE).toPath();
            final Path keyFilePath = new File(TEST_SSL_KEY_FILE).toPath();
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            when(sourceConfig.isSsl()).thenReturn(true);
            when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
            when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
            final BaseHttpSource<Record<Event>> objectUnderTest = createObjectUnderTest();
            objectUnderTest.start(testBuffer);
            objectUnderTest.stop();

            final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
            final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
            verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
            final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
            final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
            assertThat(actualCertificate, is(certAsString));
            assertThat(actualPrivateKey, is(keyAsString));
        }
    }

    @Test
    public void testDoubleStart() {
        // starting server
        httpApiSource.start(testBuffer);
        // double start server
        Assertions.assertThrows(IllegalStateException.class, () -> httpApiSource.start(testBuffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        Assertions.assertThrows(IllegalStateException.class, () -> httpApiSource.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            Assertions.assertThrows(RuntimeException.class, () -> httpApiSource.start(testBuffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> httpApiSource.start(testBuffer));
            Assertions.assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            Assertions.assertThrows(RuntimeException.class, () -> httpApiSource.start(testBuffer));
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            httpApiSource.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            Assertions.assertThrows(RuntimeException.class, httpApiSource::stop);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            httpApiSource.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = Assertions.assertThrows(RuntimeException.class, httpApiSource::stop);
            Assertions.assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final BaseHttpSource<Record<Event>> httpApiSource = createObjectUnderTest();
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            httpApiSource.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            Assertions.assertThrows(RuntimeException.class, httpApiSource::stop);
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        httpApiSource.start(testBuffer);

        final BaseHttpSource<Record<Event>> secondHttpAPISource = createObjectUnderTest();
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> secondHttpAPISource.start(testBuffer));
    }

}
