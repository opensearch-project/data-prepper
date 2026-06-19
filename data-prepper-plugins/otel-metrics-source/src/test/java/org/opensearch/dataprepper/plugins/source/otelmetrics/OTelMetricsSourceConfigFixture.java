/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.BASIC_AUTH_USERNAME;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.CONFIG_HTTP_PATH;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.RETRY_INFO;

import java.util.Map;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpData;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

public class OTelMetricsSourceConfigFixture {

    public static OTelMetricsSourceConfig createDefaultConfig() {
        return createDefaultConfigBuilder().build();
    }

    public static OTelMetricsSourceConfig.OTelMetricsSourceConfigBuilder createDefaultConfigBuilder() {
        return OTelMetricsSourceConfig.builder()
                .healthCheck(true)
                .httpPath(CONFIG_HTTP_PATH)
                .port(DEFAULT_PORT)
                .enableUnframedRequests(false)
                .ssl(false)
                .requestTimeoutInMillis(DEFAULT_REQUEST_TIMEOUT_MS)
                .maxConnectionCount(10)
                .threadCount(5)
                .retryInfo(RETRY_INFO)
                .compression(CompressionOption.NONE);
    }

    public static OTelMetricsSourceConfig.OTelMetricsSourceConfigBuilder createBuilderForConfigWithSsl() {
        return createDefaultConfigBuilder()
                .ssl(true)
                .useAcmCertForSSL(false)
                .sslKeyCertChainFile("data/certificate/test_cert.crt")
                .sslKeyFile("data/certificate/test_decrypted_key.key");
    }

    public static OTelMetricsSourceConfig.OTelMetricsSourceConfigBuilder createBuilderForConfigWithAcmSsl() {
        return createDefaultConfigBuilder()
                .ssl(true)
                .useAcmCertForSSL(true)
                .awsRegion("us-east-1")
                .acmCertificateArn("arn:aws:acm:us-east-1:account:certificate/1234-567-856456")
                .sslKeyCertChainFile("data/certificate/test_cert.crt")
                .sslKeyFile("data/certificate/test_decrypted_key.key");
    }

    public static OTelMetricsSourceConfig.OTelMetricsSourceConfigBuilder createConfigBuilderWithBasicAuth() {
        Map<String, Object> httpBasicConfig = Map.of(
                "username", BASIC_AUTH_USERNAME,
                "password", BASIC_AUTH_PASSWORD
        );
        return createDefaultConfigBuilder()
                .authentication(new PluginModel("http_basic", httpBasicConfig));
    }

    public static ExportMetricsServiceRequest createMetricsServiceRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        Metric metric = Metric.newBuilder().setGauge(gauge).setUnit("seconds")
                .setName("name")
                .setDescription("description")
                .build();

        ScopeMetrics.Builder scopeMetric = ScopeMetrics.newBuilder()
                .addMetrics(metric)
                .setScope(InstrumentationScope.newBuilder()
                        .setName("scopeName")
                        .setVersion("scopeVersion")
                        .build());

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetric)
                .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics)
                .build();
    }

    public static HttpData createJsonHttpPayload() throws InvalidProtocolBufferException {
        return HttpData.copyOf(JsonFormat.printer().print(createMetricsServiceRequest()).getBytes());
    }
}
