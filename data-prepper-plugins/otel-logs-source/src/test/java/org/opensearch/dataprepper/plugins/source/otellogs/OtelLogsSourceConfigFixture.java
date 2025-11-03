package org.opensearch.dataprepper.plugins.source.otellogs;

import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_USERNAME;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_PATH;

import java.util.Map;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;

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
                .retryInfo(new RetryInfoConfig())
                .port(DEFAULT_PORT)
                .enableUnframedRequests(false)
                .ssl(false)
                .requestTimeoutInMillis(DEFAULT_REQUEST_TIMEOUT_MS)
                .maxConnectionCount(10)
                .threadCount(5)
                .compression(CompressionOption.NONE)
                .path(CONFIG_PATH);
    }

    public static OTelLogsSourceConfig createLogsConfigWittSsl() {
        return createDefaultConfigBuilder()
                .ssl(true)
                .useAcmCertForSSL(false)
                .sslKeyCertChainFile("data/certificate/test_cert.crt")
                .sslKeyFile("data/certificate/test_decrypted_key.key")
                .build();
    }

    public static OTelLogsSourceConfig createConfigWithBasicAuth() {
        Map<String, Object> httpBasicConfig = Map.of(
                "username", BASIC_AUTH_USERNAME,
                "password", BASIC_AUTH_PASSWORD
        );
        return createDefaultConfigBuilder()
                .authentication(new PluginModel("http_basic", httpBasicConfig))
                .build();
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
