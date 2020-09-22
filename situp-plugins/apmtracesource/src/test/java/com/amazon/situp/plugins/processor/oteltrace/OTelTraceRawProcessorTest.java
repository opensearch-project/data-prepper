package com.amazon.situp.plugins.processor.oteltrace;

import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.*;

public class OTelTraceRawProcessorTest {

    @Test
    public void testGetJsonFromProtobufObj() throws InvalidProtocolBufferException {

        final long time = System.nanoTime();
        ByteBuffer traceIdBuffer = ByteBuffer.allocate(2 * Long.BYTES);
        traceIdBuffer.putLong(1);
        traceIdBuffer.putLong(2);
        ByteBuffer spanIdBuffer = ByteBuffer.allocate(Long.BYTES);
        spanIdBuffer.putLong(3);

        ResourceSpans rs = ResourceSpans.newBuilder()
                .setResource(Resource.newBuilder()
                        .addAttributes(KeyValue.newBuilder()
                                .setKey("resource-key")
                                .build()
                        )
                        .build()
                )
                .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder()
                        .setInstrumentationLibrary(InstrumentationLibrary.newBuilder()
                                .setName("test-instrumentation")
                                .setVersion("0.1.0")
                                .build()
                        )
                        .addSpans(Span.newBuilder()
                                .setTraceId(ByteString.copyFrom(traceIdBuffer.array()))
                                .setSpanId(ByteString.copyFrom(spanIdBuffer.array()))
                                .setName("test-span")
                                .setKind(Span.SpanKind.CONSUMER)
                                .setStartTimeUnixNano(time)
                                .setEndTimeUnixNano(time + 1L)
                                .setStatus(Status.newBuilder().setCodeValue(Status.StatusCode.Aborted_VALUE).setMessage("status-description").build())
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("x")
                                        .build()
                                )
                                .setDroppedAttributesCount(1)
                                .addEvents(Span.Event.newBuilder().setName("event-1").setTimeUnixNano(time + 2).build())
                                .addEvents(Span.Event.newBuilder().setName("event-2").setTimeUnixNano(time + 2).build())
                                .setDroppedEventsCount(2)
                                .build())
                        .build()
                )
                .build();

        Record<ResourceSpans> records = new Record<>(rs);
        OTelTraceRawProcessor.getJsonFromProtobufObj(records);
        assertThat(records).isNotNull();
        assertThat(records.getData().getResource().getAttributes(0).getKey().equals("resource-key"));
        assertThat(records.getData().getInstrumentationLibrarySpans(0).getInstrumentationLibrary().getName().equals("test-instrumentation"));
        assertThat(records.getData().getInstrumentationLibrarySpans(0).getSpans(0).getName().equals("test-span"));
        assertThat(records.getData().getInstrumentationLibrarySpans(0).getSpans(0).getKind().equals(Span.SpanKind.CONSUMER));
    }

    String jsonString = "{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"analytics-service\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"java\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"0.8.0-SNAPSHOT\"}}]},\"instrumentationLibrarySpans\":[{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"LoggingController.save\",\"kind\":\"INTERNAL\",\"startTimeUnixNano\":\"1597902043168792500\",\"endTimeUnixNano\":\"1597902043215953100\",\"status\":{}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"LoggingController.save\",\"kind\":\"INTERNAL\",\"startTimeUnixNano\":\"1597902046041803300\",\"endTimeUnixNano\":\"1597902046088892200\",\"status\":{}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"HTTP PUT\",\"kind\":\"CLIENT\",\"startTimeUnixNano\":\"1597902043175204700\",\"endTimeUnixNano\":\"1597902043205117100\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout=1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"status\":{}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"HTTP PUT\",\"kind\":\"CLIENT\",\"startTimeUnixNano\":\"1597902046052809200\",\"endTimeUnixNano\":\"1597902046084822500\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"/logs/_doc/service_1?timeout=1m\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"PUT\"}}],\"status\":{}}]},{\"instrumentationLibrary\":{\"name\":\"io.opentelemetry.auto.servlet-3.0\"},\"spans\":[{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"name\":\"/logs\",\"kind\":\"SERVER\",\"startTimeUnixNano\":\"1597902043168010200\",\"endTimeUnixNano\":\"1597902043217170200\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41164\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"status\":{}},{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\",\"name\":\"/logs\",\"kind\":\"SERVER\",\"startTimeUnixNano\":\"1597902046041011600\",\"endTimeUnixNano\":\"1597902046089556800\",\"attributes\":[{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"41168\"}},{\"key\":\"servlet.path\",\"value\":{\"stringValue\":\"/logs\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"7\"}},{\"key\":\"http.user_agent\",\"value\":{\"stringValue\":\"curl/7.54.0\"}},{\"key\":\"http.flavor\",\"value\":{\"stringValue\":\"HTTP/1.1\"}},{\"key\":\"servlet.context\",\"value\":{\"stringValue\":\"\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8087/logs\"}},{\"key\":\"net.peer.ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}},{\"key\":\"http.method\",\"value\":{\"stringValue\":\"POST\"}},{\"key\":\"http.client_ip\",\"value\":{\"stringValue\":\"172.29.0.1\"}}],\"status\":{}}]}]}";

    String output[] = {
            "{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"CFrAgv/Pv40=\",\"parentSpanId\":\"yxwHNNFJQP0=\",\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"kind\":\"INTERNAL\",\"status\":{},\"startTime\":\"2020-08-20T05:40:43.168792500Z\",\"endTime\":\"2020-08-20T05:40:43.215953100Z\",\"resource.attributes.service.name\":\"analytics-service\",\"resource.attributes.telemetry.sdk.language\":\"java\",\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\",\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}",
            "{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"WsV2GFh9pxw=\",\"parentSpanId\":\"mnO/qUT5ye4=\",\"name\":\"io.opentelemetry.auto.spring-webmvc-3.1\",\"kind\":\"INTERNAL\",\"status\":{},\"startTime\":\"2020-08-20T05:40:46.041803300Z\",\"endTime\":\"2020-08-20T05:40:46.088892200Z\",\"resource.attributes.service.name\":\"analytics-service\",\"resource.attributes.telemetry.sdk.language\":\"java\",\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\",\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}",
            "{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"G4PRdsC1+0Y=\",\"parentSpanId\":\"CFrAgv/Pv40=\",\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"kind\":\"CLIENT\",\"status\":{},\"startTime\":\"2020-08-20T05:40:43.175204700Z\",\"endTime\":\"2020-08-20T05:40:43.205117100Z\",\"attributes.http.status_code\":200,\"attributes.http.url\":\"/logs/_doc/service_1?timeout=1m\",\"attributes.http.method\":\"PUT\",\"resource.attributes.service.name\":\"analytics-service\",\"resource.attributes.telemetry.sdk.language\":\"java\",\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\",\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}",
            "{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"na9fwApQsYs=\",\"parentSpanId\":\"WsV2GFh9pxw=\",\"name\":\"io.opentelemetry.auto.apache-httpasyncclient-4.0\",\"kind\":\"CLIENT\",\"status\":{},\"startTime\":\"2020-08-20T05:40:46.052809200Z\",\"endTime\":\"2020-08-20T05:40:46.084822500Z\",\"attributes.http.status_code\":200,\"attributes.http.url\":\"/logs/_doc/service_1?timeout=1m\",\"attributes.http.method\":\"PUT\",\"resource.attributes.service.name\":\"analytics-service\",\"resource.attributes.telemetry.sdk.language\":\"java\",\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\",\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}",
            "{\"traceId\":\"/6V20yEXOsbO82Acj0vedQ==\",\"spanId\":\"yxwHNNFJQP0=\",\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"kind\":\"SERVER\",\"status\":{},\"startTime\":\"2020-08-20T05:40:43.168010200Z\",\"endTime\":\"2020-08-20T05:40:43.217170200Z\",\"attributes.http.status_code\":200,\"attributes.net.peer.port\":41164,\"attributes.servlet.path\":\"/logs\",\"attributes.http.response_content_length\":7,\"attributes.http.user_agent\":\"curl/7.54.0\",\"attributes.http.flavor\":\"HTTP/1.1\",\"attributes.servlet.context\":\"\",\"attributes.http.url\":\"http://0.0.0.0:8087/logs\",\"attributes.net.peer.ip\":\"172.29.0.1\",\"attributes.http.method\":\"POST\",\"attributes.http.client_ip\":\"172.29.0.1\",\"resource.attributes.service.name\":\"analytics-service\",\"resource.attributes.telemetry.sdk.language\":\"java\",\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\",\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}",
            "{\"traceId\":\"bQ/2NNEmtuwsGAOR5ntCNw==\",\"spanId\":\"mnO/qUT5ye4=\",\"name\":\"io.opentelemetry.auto.servlet-3.0\",\"kind\":\"SERVER\",\"status\":{},\"startTime\":\"2020-08-20T05:40:46.041011600Z\",\"endTime\":\"2020-08-20T05:40:46.089556800Z\",\"attributes.http.status_code\":200,\"attributes.net.peer.port\":41168,\"attributes.servlet.path\":\"/logs\",\"attributes.http.response_content_length\":7,\"attributes.http.user_agent\":\"curl/7.54.0\",\"attributes.http.flavor\":\"HTTP/1.1\",\"attributes.servlet.context\":\"\",\"attributes.http.url\":\"http://0.0.0.0:8087/logs\",\"attributes.net.peer.ip\":\"172.29.0.1\",\"attributes.http.method\":\"POST\",\"attributes.http.client_ip\":\"172.29.0.1\",\"resource.attributes.service.name\":\"analytics-service\",\"resource.attributes.telemetry.sdk.language\":\"java\",\"resource.attributes.telemetry.sdk.name\":\"opentelemetry\",\"resource.attributes.telemetry.sdk.version\":\"0.8.0-SNAPSHOT\"}"
    };

    ArrayList<String> resourceSpansList =
            new ArrayList<>(Arrays.asList(output));


    @Test
    public void testDecodeResourceSpan() throws JsonProcessingException {
        ArrayList<String> result = OTelTraceRawProcessor.decodeResourceSpan(jsonString);
        assertThat(resourceSpansList.equals(result));
    }
}
