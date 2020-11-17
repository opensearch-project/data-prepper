package com.amazon.situp.research.zipkin;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.amazon.situp.plugins.source.oteltrace.OTelTraceSource;
import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZipkinElasticToOtel {
    public static final Logger LOG = LoggerFactory.getLogger(ZipkinElasticToOtel.class);

    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Missing indexPattern as arg");
            System.exit(1);
        }
        final String indexPattern = args[0];
        final String field = args.length >= 2? args[1] : null;
        final String value = args.length >= 3? args[2] : null;
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
        final ElasticsearchReader reader = new ElasticsearchReader(indexPattern, field, value);
        final TraceServiceGrpc.TraceServiceBlockingStub client = createGRPCClient();
        System.out.println("Reading batch 0");
        List<Map<String, Object>> sources = reader.nextBatch(restHighLevelClient);
        System.out.println(String.format("Batch size: %d", sources.size()));
        System.out.println(String.format("Total number of hits: %d", reader.getTotal()));
        int i = 0;
        while (sources.size() > 0) {
            System.out.println(String.format("Processing batch %d as ExportTraceServiceRequest", i));
            try {
                final ExportTraceServiceRequest exportTraceServiceRequest = ZipkinElasticToOtelProcessor.sourcesToRequest(sources);
                client.export(exportTraceServiceRequest);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
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
