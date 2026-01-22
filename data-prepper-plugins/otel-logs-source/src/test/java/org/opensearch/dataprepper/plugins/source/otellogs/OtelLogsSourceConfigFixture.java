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

import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_USERNAME;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_HTTP_PATH;

import java.util.Map;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpData;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

public class OtelLogsSourceConfigFixture {

    public static OTelLogsSourceConfig createDefaultConfig() {
        return createDefaultConfigBuilder().build();
    }

    public static OTelLogsSourceConfig.OTelLogsSourceConfigBuilder createDefaultConfigBuilder() {
        return OTelLogsSourceConfig.builder()
                .healthCheck(true)
                .httpPath(CONFIG_HTTP_PATH)
                .port(DEFAULT_PORT)
                .enableUnframedRequests(false)
                .ssl(false)
                .requestTimeoutInMillis(DEFAULT_REQUEST_TIMEOUT_MS)
                .maxConnectionCount(10)
                .threadCount(5)
                .compression(CompressionOption.NONE);
    }

    public static OTelLogsSourceConfig.OTelLogsSourceConfigBuilder createBuilderForConfigWithSsl() {
        return createDefaultConfigBuilder()
                .ssl(true)
                .useAcmCertForSSL(false)
                .sslKeyCertChainFile("data/certificate/test_cert.crt")
                .sslKeyFile("data/certificate/test_decrypted_key.key");
    }

    public static OTelLogsSourceConfig.OTelLogsSourceConfigBuilder createBuilderForConfigWithAcmeSsl() {
        return createDefaultConfigBuilder()
                .ssl(true)
                .useAcmCertForSSL(true)
                .awsRegion("us-east-1")
                .acmCertificateArn("arn:aws:acm:us-east-1:account:certificate/1234-567-856456")
                .sslKeyCertChainFile("data/certificate/test_cert.crt")
                .sslKeyFile("data/certificate/test_decrypted_key.key");
    }

    public static OTelLogsSourceConfig.OTelLogsSourceConfigBuilder createConfigBuilderWithBasicAuth() {
        Map<String, Object> httpBasicConfig = Map.of(
                "username", BASIC_AUTH_USERNAME,
                "password", BASIC_AUTH_PASSWORD
        );
        return createDefaultConfigBuilder()
                .authentication(new PluginModel("http_basic", httpBasicConfig));
    }

    public static ExportLogsServiceRequest createLogsServiceRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        final ResourceLogs resourceLogs = ResourceLogs.newBuilder()
                .addScopeLogs(ScopeLogs.newBuilder()
                        .addLogRecords(LogRecord.newBuilder().setSeverityNumberValue(1))
                        .build())
                .setResource(resource)
                .build();

        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
    }

    public static HttpData createJsonHttpPayload() throws InvalidProtocolBufferException {
        return HttpData.copyOf(JsonFormat.printer().print(createLogsServiceRequest()).getBytes());
    }
}
