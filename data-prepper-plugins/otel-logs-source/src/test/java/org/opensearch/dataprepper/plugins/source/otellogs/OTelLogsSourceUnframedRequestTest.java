/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.UNSUPPORTED_MEDIA_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createDefaultConfigBuilder;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_GRPC_PATH;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_HTTP_PATH;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;

import io.grpc.BindableService;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;

@ExtendWith(MockitoExtension.class)
class OTelLogsSourceUnframedRequestTest {
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
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    @Mock
    private BlockingBuffer<Record<Object>> buffer;

    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelLogsSource SOURCE;
    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder().build()).build();

    @BeforeEach
    public void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_logs", "pipeline");

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void configureSource(OTelLogsSourceConfig config) {
        SOURCE = new OTelLogsSource(config, pluginMetrics, pluginFactory, pipelineDescription);
        assertInstanceOf(OTelLogsDecoder.class, SOURCE.getDecoder());
    }

    private RequestHeadersBuilder getDefaultRequestHeadersBuilder() {
        return RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21892")
                        .method(HttpMethod.POST)
                        .path(CONFIG_HTTP_PATH)
                        .contentType(MediaType.JSON_UTF_8);
    }

    @Test
    void unframedRequests_unframedRequestsAreEnabledAndHttpRequestIsSentToGrpcEndpoint_returns200() throws InvalidProtocolBufferException {
        configureSource(createDefaultConfigBuilder().enableUnframedRequests(true).build());
        SOURCE.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .path(CONFIG_GRPC_PATH)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, OK, throwable))
                .join();
    }

    @Test
    void unframedRequests_unframeRequestsAreDisabledAndHttpRequestIsSentToGrpcEndpoint_returns415() throws InvalidProtocolBufferException {
        configureSource(createDefaultConfigBuilder().enableUnframedRequests(false).build());
        SOURCE.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .path(CONFIG_GRPC_PATH)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    private void assertSecureResponseWithStatusCode(final AggregatedHttpResponse response,
                                                    final HttpStatus expectedStatus,
                                                    final Throwable throwable) {
        assertThat("Http Status", response.status(), equalTo(expectedStatus));
        assertThat("Http Response Throwable", throwable, is(nullValue()));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(hasItem("server")));
    }

}
