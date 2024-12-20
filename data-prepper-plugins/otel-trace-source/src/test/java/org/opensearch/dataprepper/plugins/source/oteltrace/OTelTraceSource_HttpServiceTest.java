/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.ClosedSessionException;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.internal.shaded.bouncycastle.util.encoders.Base64;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;

import io.grpc.BindableService;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.*;

@ExtendWith(MockitoExtension.class)
class OTelTraceSource_HttpServiceTest {
    private static final String TEST_PIPELINE_NAME = "test_pipeline";

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private GrpcServiceBuilder grpcServiceBuilder;

    @Mock
    private GrpcService grpcService;

    @Mock
    private CompletableFuture<Void> completableFuture;

    @Mock
    private PluginFactory pluginFactory;

    @Mock(lenient = true)
    private OTelTraceSourceConfig oTelTraceSourceConfig;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private GrpcAuthenticationProvider grpcAuthProvider;

    @Captor
    ArgumentCaptor<byte[]> bytesCaptor;

    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelTraceSource SOURCE;

    private static HttpBasicAuthenticationConfig PROVIDED_CONFIG = new HttpBasicAuthenticationConfig("username", "password");


    @BeforeEach
    void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.service(any(GrpcService.class), any(Function.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(grpcAuthProvider);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.exceptionHandler(any(GrpcRequestExceptionHandler.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        when(oTelTraceSourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(oTelTraceSourceConfig.isSsl()).thenReturn(false);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(oTelTraceSourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelTraceSourceConfig.getThreadCount()).thenReturn(5);
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        // default: we don't want authentication
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(null);

        configureObjectUnderTest();
        pipelineDescription = mock(PipelineDescription.class);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
    }

    @AfterEach
    void afterEach() {
        SOURCE.stop();
    }

    private void configureObjectUnderTest() {
        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_trace", "pipeline");

        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    // todo tlongo add test for invalid payload

    @Test
    void request_that_is_successful() throws Exception {
        when(buffer.isByteBuffer()).thenReturn(true);
        ExportTraceServiceRequest request = createExportTraceRequest();
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(), HttpData.copyOf(JsonFormat.printer().print(request).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertThat(response.status(), is(HttpStatus.OK)))
                .join();

        verify(buffer, times(1)).writeBytes(bytesCaptor.capture(), anyString(), anyInt());
    }

    @Test
    void providing_unauthenticated_via_config_does_not_add_the_auth_decorator() {
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Map.of()));
        SOURCE.start(buffer);

        verify(serverBuilder, times(0)).decorator(any(Function.class));
    }

    @Test
    void request_that_causes_overflow_exception_should_not_be_written_to_buffer_and_return_retry_information() throws Exception {
        Mockito.lenient().doThrow(SizeOverflowException.class).when(buffer).writeAll(any(), anyInt());
        SOURCE.start(buffer);

        makeRequestAndAssertResponse("/opentelemetry.proto.collector.trace.v1.TraceService/Export", createExportTraceRequest(), (response, throwable) -> {
            assertThat(response.status(), is(HttpStatus.INSUFFICIENT_STORAGE));
            assertResponseBodyForRetryInformation(response, "0.100s");
        });
    }

    @Test
    void request_over_http_with_ssl_enabled_fails() {
        when(oTelTraceSourceConfig.isSsl()).thenReturn(true);
        when(oTelTraceSourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelTraceSourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);

        WebClient client = WebClient.builder("http://127.0.0.1:21890")
                .build();

        CompletionException exception = assertThrows(CompletionException.class, () -> client.execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21890")
                                .method(HttpMethod.POST)
                                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
                .aggregate()
                .join());

        assertThat(exception.getCause(), instanceOf(ClosedSessionException.class));
    }

    @ParameterizedTest
    @MethodSource("generateCredentials")
    void request_with_credentials_returns_expected_status_code(AuthTestDataHolder testData) throws InvalidProtocolBufferException {
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic", Map.of("username", PROVIDED_CONFIG.getUsername(), "password",PROVIDED_CONFIG.getPassword())));
        SOURCE.start(buffer);

        makeRequestWithCredentialsAndAssertResponse("/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                createExportTraceRequest(),
                testData.providedCredentials.getOrDefault("username", null),
                testData.providedCredentials.getOrDefault("password", null),
                (response, throwable) -> assertThat(response.status(), is(testData.expectedStatus))
        );
    }

    private static Stream<Arguments> generateCredentials() {
        return Stream.of(
                arguments(named("valid credentials", new AuthTestDataHolder(Map.of("username", "username", "password","password"), HttpStatus.OK))),
                arguments(named("wrong credentials", new AuthTestDataHolder(Map.of("username", "wrong-username", "password","wrong-password"), HttpStatus.UNAUTHORIZED))),
                arguments(named("no credentials provided", new AuthTestDataHolder(Map.of(), HttpStatus.UNAUTHORIZED)))
        );
    }

    static class AuthTestDataHolder {
        Map<String, String> providedCredentials;
        HttpStatus expectedStatus;

        public AuthTestDataHolder(Map<String, String> providedCredentials, HttpStatus expectedStatus) {
            this.providedCredentials = providedCredentials;
            this.expectedStatus = expectedStatus;
        }
    }

    void makeRequestWithCredentialsAndAssertResponse(
            String path,
            ExportTraceServiceRequest request,
            String username,
            String password,
            BiConsumer<AggregatedHttpResponse, Throwable> assertionFunction) throws InvalidProtocolBufferException {

        WebClient.of().execute(RequestHeaders.builder().add("Authorization", "Basic " + new String(Base64.encode(String.format("%s:%s", username, password).getBytes())))
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path(path)
                        .contentType(MediaType.JSON_UTF_8)
                        .build(), HttpData.copyOf(JsonFormat.printer().print(request).getBytes()))
                .aggregate()
                .whenComplete(assertionFunction)
                .join();
    }

    private void makeRequestAndAssertResponse(String path, ExportTraceServiceRequest request, BiConsumer<AggregatedHttpResponse, Throwable> assertionFunction) throws InvalidProtocolBufferException {

        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path(path)
                        .contentType(MediaType.JSON_UTF_8)
                        .build(), HttpData.copyOf(JsonFormat.printer().print(request).getBytes()))
                .aggregate()
                .whenComplete(assertionFunction)
                .join();
    }

    private ExportTraceServiceRequest createExportTraceRequest() {
        final io.opentelemetry.proto.trace.v1.Span testSpan = Span.newBuilder()
                .setTraceId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setSpanId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setName(UUID.randomUUID().toString())
                .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                .setStartTimeUnixNano(100)
                .setEndTimeUnixNano(101)
                .setTraceState("SUCCESS").build();

        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(testSpan)).build())
                .build();
    }

    private void assertResponseBodyForRetryInformation(final AggregatedHttpResponse response, String expectedDelay) {
        String body = response.content(StandardCharsets.UTF_8);

        // todo tlongo map to numeric value when creating status in exception handler
        assertThat(body, hasJsonPath("$.details[0].retryDelay", equalTo(expectedDelay)));
    }
}
