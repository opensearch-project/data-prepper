package com.amazon.situp.plugins.processor.oteltrace;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OTelTraceRawProcessorTest {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final OTelTraceRawProcessor oTelTraceRawProcessor = new OTelTraceRawProcessor(new PluginSetting(null, Collections.EMPTY_MAP));

    @Test
    public void testEmptyCollection() {
        assertThat(oTelTraceRawProcessor.execute(Collections.EMPTY_LIST)).isEmpty();
    }

    @Test
    public void testEmptyTraceRequests() {
        assertThat(oTelTraceRawProcessor.execute(Arrays.asList(new Record<>(ExportTraceServiceRequest.newBuilder().build()),
                new Record<>(ExportTraceServiceRequest.newBuilder().build())))).isEmpty();
    }

    @Test
    public void testEmptySpans() {
        assertThat(oTelTraceRawProcessor.execute(Arrays.asList(new Record<>(ExportTraceServiceRequest.newBuilder().build()),
                new Record<>(ExportTraceServiceRequest.newBuilder().build())))).isEmpty();
    }

    @Test
    public void testExportRequest() throws InvalidProtocolBufferException, JsonProcessingException {
        final String sampleRequest = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"traceState\":\"\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"Ok\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"traceState\":\"\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"Ok\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"traceState\":\"\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"Ok\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"traceState\":\"\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"Ok\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SERVER\",\"startTimeUnixNano\":\"1597902043168010200\",\"endTimeUnixNano\":\"1597902043217170200\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41164\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"Ok\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SERVER\",\"startTimeUnixNano\":\"1597902046041011600\",\"endTimeUnixNano\":\"1597902046089556800\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41168\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"Ok\",\"message\":\"\"}}]}]}]}";
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(sampleRequest, builder);
        final ExportTraceServiceRequest exportTraceServiceRequest = builder.build();
        final List<Record<String>> processedRecords = (List<Record<String>>) oTelTraceRawProcessor.execute(Collections.singletonList(new Record<>(exportTraceServiceRequest)));
        assertThat(processedRecords.size()).isEqualTo(6);
    }
}
