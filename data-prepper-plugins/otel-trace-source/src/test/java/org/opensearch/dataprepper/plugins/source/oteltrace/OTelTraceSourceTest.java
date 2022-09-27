/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
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
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.SSL;

@ExtendWith(MockitoExtension.class)
public class OTelTraceSourceTest {

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private GrpcServiceBuilder grpcServiceBuilder;

    @Mock
    private GrpcService grpcService;

    @Mock
    private CertificateProviderFactory certificateProviderFactory;

    @Mock
    private CertificateProvider certificateProvider;

    @Mock
    private Certificate certificate;

    @Mock
    private CompletableFuture<Void> completableFuture;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    private PluginSetting pluginSetting;
    private PluginSetting testPluginSetting;
    private OTelTraceSourceConfig oTelTraceSourceConfig;
    private PluginMetrics pluginMetrics;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private BlockingBuffer<Record<Object>> buffer;

    private static final ExportTraceServiceRequest SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder()
                            .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder().setTraceState("SUCCESS").build())).build()).build();
    private OTelTraceSource SOURCE;
    private static final ExportTraceServiceRequest FAILURE_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder()
                            .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder().setTraceState("FAILURE").build())).build()).build();

    private static void assertStatusCode415AndNoServerHeaders(final AggregatedHttpResponse response, final Throwable throwable) {
        assertThat("Http Status", response.status(), is(HttpStatus.UNSUPPORTED_MEDIA_TYPE));
        assertThat("Http Response Throwable", throwable, is(nullValue()));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
    }

    private BlockingBuffer<Record<Object>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }

    private void configureObjectUnderTest() {
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("request_timeout", 5);
        settingsMap.put(SSL, false);
        pluginSetting = new PluginSetting("otel_trace", settingsMap);
        pluginSetting.setPipelineName("pipeline");
        pluginMetrics = PluginMetrics.fromNames("otel_trace", "pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTraceSourceConfig.class);
        SOURCE = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
    }

    @BeforeEach
    public void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.service(any(GrpcService.class), any(Function.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        lenient().when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        configureObjectUnderTest();
        buffer = getBuffer();
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    @Test
    void testHttpFullJson() throws InvalidProtocolBufferException {
        configureObjectUnderTest();
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(OTelTraceSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(OTelTraceSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
    }

    @Test
    void testHttpsFullJson() throws InvalidProtocolBufferException {

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("request_timeout", 5);
        settingsMap.put(SSL, true);
        settingsMap.put("useAcmCertForSSL", false);
        settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
        settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
        pluginSetting = new PluginSetting("otel_trace", settingsMap);
        pluginSetting.setPipelineName("pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTraceSourceConfig.class);
        SOURCE = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);

        buffer = getBuffer();
        SOURCE.start(buffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(OTelTraceSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(OTelTraceSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
    }

    @Test
    void testHttpFullBytes() {
        configureObjectUnderTest();
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(SUCCESS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete(OTelTraceSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(FAILURE_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete(OTelTraceSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
    }

    @Test
    public void testServerStartCertFileSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);

            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", false);
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
            source.start(buffer);
            source.stop();

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
    public void testServerStartACMCertSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);
            source.start(buffer);
            source.stop();

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
    void start_with_Health_configured_includes_HealthCheck_service() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class);
            MockedStatic<GrpcService> grpcServerMock = Mockito.mockStatic(GrpcService.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            grpcServerMock.when(GrpcService::builder).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.addService(any(ServerServiceDefinition.class))).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
            settingsMap.put("health_check_service", "true");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");

            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    void start_with_Health_configured_unframed_requests_includes_HTTPHealthCheck_service() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class);
             MockedStatic<GrpcService> grpcServerMock = Mockito.mockStatic(GrpcService.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            grpcServerMock.when(GrpcService::builder).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.addService(any(ServerServiceDefinition.class))).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
            settingsMap.put("health_check_service", "true");
            settingsMap.put("unframed_requests", "true");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");

            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
        verify(serverBuilder).service(eq("/health"), isA(HealthCheckService.class));
    }

    @Test
    void start_without_Health_configured_does_not_include_HealthCheck_service() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class);
            MockedStatic<GrpcService> grpcServerMock = Mockito.mockStatic(GrpcService.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            grpcServerMock.when(GrpcService::builder).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.addService(any(ServerServiceDefinition.class))).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
            settingsMap.put("health_check_service", "false");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    void start_without_Health_configured_unframed_requests_does_not_include_HealthCheck_service() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class);
             MockedStatic<GrpcService> grpcServerMock = Mockito.mockStatic(GrpcService.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            grpcServerMock.when(GrpcService::builder).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.addService(any(ServerServiceDefinition.class))).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
            settingsMap.put("health_check_service", "false");
            settingsMap.put("unframed_requests", "true");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    public void testHealthCheckUnauthNotAllowed() {
        // Prepare
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(SSL, false);
        settingsMap.put("health_check_service", "true");
        settingsMap.put("unframed_requests", "true");
        settingsMap.put("proto_reflection_service", "true");
        settingsMap.put("unauthenticated_health_check", "false");
        settingsMap.put("authentication", new PluginModel("http_basic",
                new HashMap<>()
                {
                    {
                        put("username", "test");
                        put("password", "test2");
                    }
                }));

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);

        source.start(buffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("localhost:21890")
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.UNAUTHORIZED)).join();

        source.stop();
    }

    @Test
    public void testHealthCheckUnauthAllowed() {
        // Prepare
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(SSL, false);
        settingsMap.put("health_check_service", "true");
        settingsMap.put("unframed_requests", "true");
        settingsMap.put("proto_reflection_service", "true");
        settingsMap.put("unauthenticated_health_check", "true");
        settingsMap.put("authentication", new PluginModel("http_basic",
                new HashMap<>()
                {
                    {
                        put("username", "test");
                        put("password", "test2");
                    }
                }));

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);

        source.start(buffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("localhost:21890")
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        source.stop();
    }

    private void assertSecureResponseWithStatusCode(final AggregatedHttpResponse response, final HttpStatus expectedStatus) {
        assertThat("Http Status", response.status(), equalTo(expectedStatus));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
    }

    @Test
    public void testOptionalHttpAuthServiceNotInPlace() {
        when(server.stop()).thenReturn(completableFuture);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            SOURCE.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
        verify(serverBuilder, never()).decorator(isA(Function.class));
    }

    @Test
    public void testOptionalHttpAuthServiceInPlace() {
        final Optional<Function<? super HttpService, ? extends HttpService>> function = Optional.of(httpService -> httpService);

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("authentication", new PluginModel("test", null));
        settingsMap.put("unauthenticated_health_check", true);

        settingsMap.put(SSL, false);

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        when(authenticationProvider.getHttpAuthenticationService()).thenReturn(function);

        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
        verify(serverBuilder).decorator(isA(String.class), isA(Function.class));
    }

    @Test
    public void testOptionalHttpAuthServiceInPlaceWithUnauthenticatedDisabled() {
        final Optional<Function<? super HttpService, ? extends HttpService>> function = Optional.of(httpService -> httpService);

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("authentication", new PluginModel("test", null));
        settingsMap.put("unauthenticated_health_check", false);

        settingsMap.put(SSL, false);

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        when(authenticationProvider.getHttpAuthenticationService()).thenReturn(function);

        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
        verify(serverBuilder).decorator(isA(Function.class));
    }

    @Test
    public void testDoubleStart() {
        // starting server
        SOURCE.start(buffer);
        // double start server
        Assertions.assertThrows(IllegalStateException.class, () -> SOURCE.start(buffer));
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        SOURCE.start(buffer);

        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> source.start(buffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
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
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, source::stop);
            assertTrue(Thread.interrupted());
        }
    }
}
