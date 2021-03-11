package com.amazon.dataprepper.plugins.prepper.oteltrace;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micrometer.core.instrument.Measurement;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class OTelTraceRawPrepperTest {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long TEST_GC_INTERVAL = 3L;
    private static final long TEST_PARENT_SPAN_FLUSH_DELAY = 1L;

    PluginSetting pluginSetting;
    public OTelTraceRawPrepper oTelTraceRawPrepper;
    public ExecutorService executorService;

    @Before
    public void setup() {
        MetricsTestUtil.initMetrics();
        pluginSetting = new PluginSetting(
                "OTelTrace",
                new HashMap<String, Object>() {{
                    put(OtelTraceRawPrepperConfig.GC_INTERVAL, TEST_GC_INTERVAL);
                    put(OtelTraceRawPrepperConfig.PARENT_SPAN_FLUSH_DELAY, TEST_PARENT_SPAN_FLUSH_DELAY);
                }});
        pluginSetting.setPipelineName("pipelineOTelTrace");
        oTelTraceRawPrepper = new OTelTraceRawPrepper(pluginSetting);
        executorService = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void testResourceSpansProcessingErrorMetrics() {
        ExportTraceServiceRequest mockData = mock(ExportTraceServiceRequest.class);
        Record record = new Record(mockData);
        ResourceSpans mockResourceSpans = mock(ResourceSpans.class);
        List<ResourceSpans> mockResourceSpansList = Collections.singletonList(mockResourceSpans);

        when(mockData.getResourceSpansList()).thenReturn(mockResourceSpansList);
        when(mockResourceSpans.getResource()).thenThrow(new RuntimeException());

        oTelTraceRawPrepper.doExecute(Collections.singletonList(record));

        final List<Measurement> resourceSpansErrorsMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("pipelineOTelTrace").add("OTelTrace")
                        .add(OTelTraceRawPrepper.RESOURCE_SPANS_PROCESSING_ERRORS).toString());
        final List<Measurement> totalErrorsMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("pipelineOTelTrace").add("OTelTrace")
                        .add(OTelTraceRawPrepper.TOTAL_PROCESSING_ERRORS).toString());

        Assert.assertEquals(1, resourceSpansErrorsMeasurement.size());
        Assert.assertEquals(1.0, resourceSpansErrorsMeasurement.get(0).getValue(), 0);
        Assert.assertEquals(1, totalErrorsMeasurement.size());
        Assert.assertEquals(1.0, totalErrorsMeasurement.get(0).getValue(), 0);
    }

    @Test
    public void testEmptyCollection() {
        assertThat(oTelTraceRawPrepper.doExecute(Collections.EMPTY_LIST)).isEmpty();
    }

    @Test
    public void testEmptyTraceRequests() {
        assertThat(oTelTraceRawPrepper.doExecute(Arrays.asList(new Record<>(ExportTraceServiceRequest.newBuilder().build()),
                new Record<>(ExportTraceServiceRequest.newBuilder().build())))).isEmpty();
    }

    @Test
    public void testEmptySpans() {
        assertThat(oTelTraceRawPrepper.doExecute(Arrays.asList(new Record<>(ExportTraceServiceRequest.newBuilder().build()),
                new Record<>(ExportTraceServiceRequest.newBuilder().build())))).isEmpty();
    }

    @Test
    public void testExportRequestFlushByParentSpan() throws InvalidProtocolBufferException, JsonProcessingException, InterruptedException {
        final String sampleRequest = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"traceState\":\"\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"traceState\":\"\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"traceState\":\"\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"traceState\":\"\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SPAN_KIND_SERVER\",\"startTimeUnixNano\":\"1597902043168010200\",\"endTimeUnixNano\":\"1597902043217170200\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41164\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SPAN_KIND_SERVER\",\"startTimeUnixNano\":\"1597902046041011600\",\"endTimeUnixNano\":\"1597902046089556800\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41168\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]}]}]}";
        final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJson(sampleRequest);
        oTelTraceRawPrepper.doExecute(Collections.singletonList(new Record<>(exportTraceServiceRequest)));
        await().atMost(2 * TEST_PARENT_SPAN_FLUSH_DELAY, TimeUnit.SECONDS).untilAsserted(() -> {
            final List<Record<String>> processedRecords = (List<Record<String>>) oTelTraceRawPrepper.doExecute(Collections.emptyList());
            Assertions.assertThat(processedRecords.size()).isEqualTo(6);
            Assertions.assertThat(getOrphanedSpanCount(processedRecords)).isEqualTo(0);
        });
    }

    @Test
    public void testExportRequestFlushByParentSpanMultiThread() throws InvalidProtocolBufferException, InterruptedException, ExecutionException {
        final String sampleRequestWithTraceGroupInterleaved1 = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"traceState\":\"\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"traceState\":\"\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SPAN_KIND_SERVER\",\"startTimeUnixNano\":\"1597902043168010200\",\"endTimeUnixNano\":\"1597902043217170200\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41164\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]}]}]}";
        final String sampleRequestWithTraceGroupInterleaved2 = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"traceState\":\"\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"traceState\":\"\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SPAN_KIND_SERVER\",\"startTimeUnixNano\":\"1597902046041011600\",\"endTimeUnixNano\":\"1597902046089556800\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41168\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]}]}]}";
        final ExportTraceServiceRequest exportTraceServiceRequest1 = buildExportTraceServiceRequestFromJson(sampleRequestWithTraceGroupInterleaved1);
        final ExportTraceServiceRequest exportTraceServiceRequest2 = buildExportTraceServiceRequestFromJson(sampleRequestWithTraceGroupInterleaved2);
        final List<Record<String>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest1)));
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest2)));
        for (Future<Collection<Record<String>>> future : futures) {
            processedRecords.addAll(future.get());
        }
        await().atMost(2 * TEST_PARENT_SPAN_FLUSH_DELAY, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Future<Collection<Record<String>>>> futureList = submitExportTraceServiceRequests(Collections.emptyList());
            for (Future<Collection<Record<String>>> future : futureList) {
                processedRecords.addAll(future.get());
            }
            Assertions.assertThat(processedRecords.size()).isEqualTo(6);
            Assertions.assertThat(getOrphanedSpanCount(processedRecords)).isEqualTo(0);
        });
    }

    @Test
    public void testExportRequestFlushByGC() throws InvalidProtocolBufferException, InterruptedException {
        final String sampleRequestWithOrphanedSpans = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"traceState\":\"\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"traceState\":\"\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"traceState\":\"\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"traceState\":\"\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]}]}]}";
        final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJson(sampleRequestWithOrphanedSpans);
        oTelTraceRawPrepper.doExecute(Collections.singletonList(new Record<>(exportTraceServiceRequest)));
        await().atMost(2 * TEST_GC_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            final List<Record<String>> processedRecords = (List<Record<String>>) oTelTraceRawPrepper.doExecute(Collections.emptyList());
            Assertions.assertThat(processedRecords.size()).isEqualTo(4);
            Assertions.assertThat(getOrphanedSpanCount(processedRecords)).isEqualTo(4);
        });
    }

    @Test
    public void testExportRequestFlushByMixedMultiThread() throws InvalidProtocolBufferException, InterruptedException, ExecutionException {
        final String singleTraceGroupSampleRequest = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"traceState\":\"\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"traceState\":\"\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"traceState\":\"\",\"parentSpanId\":\"\",\"name\":\"/logs\",\"kind\":\"SPAN_KIND_SERVER\",\"startTimeUnixNano\":\"1597902043168010200\",\"endTimeUnixNano\":\"1597902043217170200\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41164\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]}]}]}";
        final String singleTraceGroupSampleRequestRootSpanMissing = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}],\"droppedAttributesCount\":0},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"version\":\"\"},\"spans\":[{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"traceState\":\"\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"SPAN_KIND_INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"attributes\":[],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"version\":\"\"},\"spans\":[{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"traceState\":\"\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"SPAN_KIND_CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout\\u003d1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"droppedAttributesCount\":0,\"events\":[],\"droppedEventsCount\":0,\"links\":[],\"droppedLinksCount\":0,\"status\":{\"code\":\"STATUS_CODE_OK\",\"message\":\"\"}}]}]}]}";
        final ExportTraceServiceRequest exportTraceServiceRequest1 = buildExportTraceServiceRequestFromJson(singleTraceGroupSampleRequest);
        final ExportTraceServiceRequest exportTraceServiceRequest2 = buildExportTraceServiceRequestFromJson(singleTraceGroupSampleRequestRootSpanMissing);
        List<Record<String>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest1)));
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest2)));
        for (Future<Collection<Record<String>>> future : futures) {
            processedRecords.addAll(future.get());
        }
        await().atMost(2 * TEST_GC_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Future<Collection<Record<String>>>> futureList = submitExportTraceServiceRequests(Collections.emptyList());
            for (Future<Collection<Record<String>>> future : futureList) {
                processedRecords.addAll(future.get());
            }
            Assertions.assertThat(processedRecords.size()).isEqualTo(5);
            Assertions.assertThat(getOrphanedSpanCount(processedRecords)).isEqualTo(2);
        });
    }

    private ExportTraceServiceRequest buildExportTraceServiceRequestFromJson(String requestJson) throws InvalidProtocolBufferException {
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(requestJson, builder);
        return builder.build();
    }

    private List<Future<Collection<Record<String>>>> submitExportTraceServiceRequests(Collection<ExportTraceServiceRequest> exportTraceServiceRequests) {
        final List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        final List<Record<ExportTraceServiceRequest>> records = exportTraceServiceRequests.stream().map(Record::new).collect(Collectors.toList());
        futures.add(executorService.submit(() -> oTelTraceRawPrepper.doExecute(records)));
        return futures;
    }

    private int getOrphanedSpanCount(List<Record<String>> records) throws JsonProcessingException {
        int count = 0;
        for (Record<String> record: records) {
            String spanJson = record.getData();
            Map<String, Object> spanMap = OBJECT_MAPPER.readValue(spanJson, new TypeReference<Map<String, Object>>() {});
            if (spanMap.get("traceGroup") == null) {
                count += 1;
            }
        }
        return count;
    }
}

