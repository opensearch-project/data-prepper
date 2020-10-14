package com.amazon.situp.integration;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.model.source.Source;
import com.amazon.situp.pipeline.Pipeline;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import com.amazon.situp.plugins.processor.oteltrace.OTelTraceRawProcessor;
import com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.amazon.situp.plugins.sink.elasticsearch.ElasticsearchSink;
import com.amazon.situp.plugins.sink.elasticsearch.IndexConfiguration;
import com.amazon.situp.plugins.source.apmtracesource.ApmTraceSource;
import com.amazon.situp.plugins.source.oteltracesource.OTelTraceSource;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Assert;
import org.junit.Test;

public class EndToEndIntegrationTest extends ESRestTestCase {

    @Test
    public void testyTest() {

    }

    public static List<String> HOSTS = Arrays.stream(System.getProperty("tests.rest.cluster").split(","))
            .map(ip -> "http://" + ip).collect(Collectors.toList());


    private static BlockingBuffer<Record<ExportTraceServiceRequest>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }

    private PluginSetting generatePluginSetting(final boolean isRaw, final boolean isServiceMap, final String indexAlias, final String templateFilePath) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.TRACE_ANALYTICS_RAW_FLAG, isRaw);
        metadata.put(IndexConfiguration.TRACE_ANALYTICS_SERVICE_MAP_FLAG, isServiceMap);
        metadata.put(ConnectionConfiguration.HOSTS, HOSTS);
        metadata.put(IndexConfiguration.INDEX_ALIAS, indexAlias);
        metadata.put(IndexConfiguration.TEMPLATE_FILE, templateFilePath);

        return new PluginSetting("elasticsearch", metadata);
    }

    public void testPipelineEndToEnd() throws IOException, InterruptedException {
        //Create otel trace source
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("request_timeout", 1);
        final OTelTraceSource oTelTraceSource = new OTelTraceSource(new PluginSetting("otel_trace_source", integerHashMap));

        //Create processors if necessary
        final OTelTraceRawProcessor oTelTraceRawProcessor = new OTelTraceRawProcessor();

        //Create Elasticsearch sink
        final PluginSetting pluginSetting = generatePluginSetting(true, false, "testIndex", null);
        ElasticsearchSink sink = new ElasticsearchSink(pluginSetting);
        //Create pipeline

        final Pipeline pipeline = new Pipeline(
                "integTestPipeline",
                oTelTraceSource,
                getBuffer(),
                Collections.singletonList(oTelTraceRawProcessor),
                Collections.singletonList(sink),
                1, 100
        );

        //Start pipeline
        pipeline.execute();

        //Create async process to send data to apm source

        final ExportTraceServiceRequest exportTraceServiceRequest = getExportTraceServiceRequest(
                getResourceSpans(
                        "SERVICE",
                        "SPAN",
                        new byte[]{0,0,0,0,0,0,0,0},
                        new byte[]{0,0,0,0,0,0,0,0},
                        new byte[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
                        Span.SpanKind.CLIENT
                )
        );

        final AggregatedHttpResponse res = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(exportTraceServiceRequest).getBytes())).aggregate().join();
        //Wait

        Thread.sleep(1000);

        //Verify data in elasticsearch sink
        Response response = client().performRequest(new Request("GET", "/_search"));
        System.out.println(response.toString());
    }


    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind) throws UnsupportedEncodingException {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("service.name")
                                        .setValue(AnyValue.newBuilder().setStringValue(serviceName).build()).build())
                                .build()
                )
                .addInstrumentationLibrarySpans(
                        0,
                        InstrumentationLibrarySpans.newBuilder()
                                .addSpans(
                                        Span.newBuilder()
                                                .setName(spanName)
                                                .setKind(spanKind)
                                                .setSpanId(ByteString.copyFrom(spanId))
                                                .setParentSpanId(parentSpanId)
                                                .setTraceId(ByteString.copyFrom(traceId))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(ResourceSpans...spans){
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(Arrays.asList(spans))
                .build();
    }


}
