package org.opensearch.dataprepper.plugins.source.otellogs;

import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_PATH;

import java.util.Map;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpData;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;

public class OtelLogsSourceConfigFixture {
    public static OTelLogsSourceConfig createLogsConfigWithoutSsl() {
        return OTelLogsSourceConfig.builder()
                .port(DEFAULT_PORT)
                .enableUnframedRequests(false)
                .ssl(false)
                .requestTimeoutInMillis(DEFAULT_REQUEST_TIMEOUT_MS)
                .maxConnectionCount(10)
                .threadCount(5)
                .compression(CompressionOption.NONE)
                .path(CONFIG_PATH) // todo tlongo chekc path
                .build();
    }

    public static OTelLogsSourceConfig createConfigWithBasicAuth() {
        Map<String, Object> httpBasicConfig = Map.of("http_basic", Map.of("username", "test", "password", ""));
        PluginModel authentication = new PluginModel("authentication", httpBasicConfig);
        return OTelLogsSourceConfig.builder()
                .port(DEFAULT_PORT)
                .ssl(false)
                .requestTimeoutInMillis(DEFAULT_REQUEST_TIMEOUT_MS)
                .maxConnectionCount(10)
                .threadCount(5)
                .compression(CompressionOption.NONE)
                .path(CONFIG_PATH) // todo tlongo chekc path
                .authentication(authentication)
                .build();
    }

    public static ExportLogsServiceRequest createLogsServiceRequest() {
        return  ExportLogsServiceRequest
                .newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder().build())
                .build();
    }

    public static HttpData createJsonHttpPayload() throws InvalidProtocolBufferException {
        return HttpData.copyOf(JsonFormat.printer().print(createLogsServiceRequest()).getBytes());
    }
}
