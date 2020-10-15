package com.amazon.situp.research.zipkin;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.amazon.situp.plugins.source.oteltracesource.OTelTraceSource;
import com.linecorp.armeria.client.Clients;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZipkinElasticToOtel {
    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Missing indexPattern as arg");
            System.exit(1);
        }
        final String indexPattern = args[0];
        final boolean isTest = !System.getProperty("test", "false").equalsIgnoreCase("false");
        OTelTraceSource oTelTraceSource = null;
        if (isTest) {
            System.out.println("Setting up testing OtelTraceSource");
            oTelTraceSource = setUpOtelTraceSource();
        }
        final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://localhost:9200"))
                .withUsername("admin")
                .withPassword("admin")
                .build();
        final RestHighLevelClient restHighLevelClient = connectionConfiguration.createClient();
        final ElasticsearchReader reader = new ElasticsearchReader(indexPattern);
        final TraceServiceGrpc.TraceServiceBlockingStub client = createGRPCClient();
        System.out.println("Reading batch 0");
        List<Map<String, Object>> sources = reader.nextBatch(restHighLevelClient);
        System.out.println(String.format("Batch size: %d", sources.size()));
        System.out.println(String.format("Total number of hits: %d", reader.getTotal()));
        int i = 0;
        while (i < 5 && sources.size() > 0) {
            System.out.println(String.format("Processing batch %d as ExportTraceServiceRequest", i));
            final ExportTraceServiceRequest exportTraceServiceRequest = ZipkinElasticToOtelProcessor.sourcesToRequest(sources);
            client.export(exportTraceServiceRequest);
            System.out.println(String.format("Reading batch %d", i+1));
            sources = reader.nextBatch(restHighLevelClient);
            i++;
        }
        System.out.println("Clearing reader scroll context ...");
        reader.clearScroll(restHighLevelClient);
        System.out.println("Closing REST client");
        restHighLevelClient.close();
        if (isTest) {
            closeOtelTraceSource(oTelTraceSource);
        }
    }

    public static TraceServiceGrpc.TraceServiceBlockingStub createGRPCClient() {
        return Clients.newClient("gproto+http://127.0.0.1:21890/", TraceServiceGrpc.TraceServiceBlockingStub.class);
    }

    public static OTelTraceSource setUpOtelTraceSource() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("request_timeout", 1);
        final OTelTraceSource SOURCE = new OTelTraceSource(new PluginSetting("otel_trace_source", integerHashMap));
        SOURCE.start(getBuffer());
        return SOURCE;
    }

    public static void closeOtelTraceSource(final OTelTraceSource oTelTraceSource) {
        oTelTraceSource.stop();
    }

    private static BlockingBuffer<Record<ExportTraceServiceRequest>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 5);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }
}
