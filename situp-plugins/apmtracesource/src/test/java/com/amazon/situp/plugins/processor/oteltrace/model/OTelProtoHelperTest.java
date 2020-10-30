package com.amazon.situp.plugins.processor.oteltrace.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class OTelProtoHelperTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    private Map<String, Object> returnMap(final String jsonStr) throws JsonProcessingException {
       return (Map<String, Object>) OBJECT_MAPPER.readValue(jsonStr, Map.class);
    }

    private List<Object> returnList(final String jsonStr) throws JsonProcessingException {
        return (List<Object>) OBJECT_MAPPER.readValue(jsonStr, List.class);
    }

    /**
     * Below object has a KeyValue with a key mapped to KeyValueList and is part of the span attributes
     * @throws JsonProcessingException
     */
    @Test
    public void testKeyValueListAsSpanAttributes() throws JsonProcessingException {

       final KeyValue childAttr1 = KeyValue.newBuilder().setKey("statement").setValue(AnyValue.newBuilder()
                .setIntValue(1_000).build()).build();
       final KeyValue childAttr2 = KeyValue.newBuilder().setKey("statement.params").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
       final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("db.details").setValue(AnyValue.newBuilder()
                .setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2)).build()).build()).build();
       final KeyValue spanAttribute2 = KeyValue.newBuilder().setKey("http.status").setValue(AnyValue.newBuilder()
                .setStringValue("4xx").build()).build();

       final Map<String, Object> actual = OTelProtoHelper.getSpanAttributes(Span.newBuilder()
                .addAllAttributes(Arrays.asList(spanAttribute1, spanAttribute2)).build());
       assertThat(actual.get(OTelProtoHelper.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute2.getKey()))
                .equals(spanAttribute2.getValue().getStringValue())).isTrue();
       assertThat(actual.containsKey(OTelProtoHelper.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey()))).isTrue();
       final Map<String, Object> actualValue = returnMap((String)actual
                .get(OTelProtoHelper.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())));
       assertThat((Integer) actualValue.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))==childAttr1.getValue().getIntValue()).isTrue();
       assertThat(actualValue.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())).equals(childAttr2.getValue().getStringValue())).isTrue();
    }

    /**
     * Below object has a KeyValue with a key mapped to KeyValueList and is part of the resource attributes
     * @throws JsonProcessingException
     */
    @Test
    public void testKeyValueListAsResourceAttributes() throws JsonProcessingException {
        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("ec2.instances").setValue(AnyValue.newBuilder()
                .setIntValue(20).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("ec2.instance.az").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("aws.details").setValue(AnyValue.newBuilder()
                .setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2)).build()).build()).build();
        final KeyValue spanAttribute2 = KeyValue.newBuilder().setKey("service.name").setValue(AnyValue.newBuilder()
                .setStringValue("EaglesService").build()).build();

        final Map<String, Object> actual = OTelProtoHelper.getResourceAttributes(Resource.newBuilder()
                .addAllAttributes(Arrays.asList(spanAttribute1, spanAttribute2)).build());
        assertThat(actual.get(OTelProtoHelper.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute2.getKey()))
                .equals(spanAttribute2.getValue().getStringValue())).isTrue();
        assertThat(actual.containsKey(OTelProtoHelper.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey()))).isTrue();
        final Map<String, Object> actualValue = returnMap((String)actual
                .get(OTelProtoHelper.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())));
        assertThat((Integer) actualValue.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))==childAttr1.getValue().getIntValue()).isTrue();
        assertThat(actualValue.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())).equals(childAttr2.getValue().getStringValue())).isTrue();

    }


    /**
     * Below object has a KeyValue with a key mapped to KeyValueList and is part of the span attributes
     * @throws JsonProcessingException
     */
    @Test
    public void testArrayOfValueAsResourceAttributes() throws JsonProcessingException {
        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("ec2.instances").setValue(AnyValue.newBuilder()
                .setIntValue(20).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("ec2.instance.az").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final AnyValue anyValue1 = AnyValue.newBuilder().setStringValue("Kibana").build();
        final AnyValue anyValue2 = AnyValue.newBuilder().setDoubleValue(2000.123).build();
        final AnyValue anyValue3 = AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2))).build();
        final ArrayValue arrayValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(anyValue1, anyValue2, anyValue3)).build();
        final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("aws.details").setValue(AnyValue.newBuilder()
                .setArrayValue(arrayValue)).build();

        final Map<String, Object> actual = OTelProtoHelper.getResourceAttributes(Resource.newBuilder()
                .addAllAttributes(Collections.singletonList(spanAttribute1)).build());
        assertThat(actual.containsKey(OTelProtoHelper.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey()))).isTrue();
        final List<Object> actualValue = returnList((String)actual
                .get(OTelProtoHelper.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())));
        assertThat((actualValue.get(0)).equals(anyValue1.getStringValue())).isTrue();
        assertThat(((Double)actualValue.get(1))==(anyValue2.getDoubleValue())).isTrue();
        final Map<String, Object> map = returnMap((String)actualValue.get(2));
        assertThat((Integer) map.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))==childAttr1.getValue().getIntValue()).isTrue();
        assertThat(map.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())).equals(childAttr2.getValue().getStringValue())).isTrue();
        assertThat((Integer) map.get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))==(childAttr1.getValue().getIntValue())).isTrue();

    }


    @Test
    public void testInstrumentationLibraryAttributes(){
        final InstrumentationLibrary il1 = InstrumentationLibrary.newBuilder().setName("Jaeger").setVersion("0.6.0").build();
        final InstrumentationLibrary il2 = InstrumentationLibrary.newBuilder().setName("Jaeger").build();
        final InstrumentationLibrary il3 = InstrumentationLibrary.newBuilder().setVersion("0.6.0").build();
        final InstrumentationLibrary il4 = InstrumentationLibrary.newBuilder().build();

        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il1).size()==2).isTrue();
        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il1).get(OTelProtoHelper.INSTRUMENTATION_LIBRARY_NAME).equals(il1.getName())).isTrue();
        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il1).get(OTelProtoHelper.INSTRUMENTATION_LIBRARY_VERSION).equals(il1.getVersion())).isTrue();

        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il2).size()==1).isTrue();
        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il2).get(OTelProtoHelper.INSTRUMENTATION_LIBRARY_NAME).equals(il2.getName())).isTrue();

        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il3).size()==1).isTrue();
        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il3).get(OTelProtoHelper.INSTRUMENTATION_LIBRARY_VERSION).equals(il3.getVersion())).isTrue();

        assertThat(OTelProtoHelper.getInstrumentationLibraryAttributes(il4).isEmpty()).isTrue();
    }

    @Test
    public void testStatusAttributes(){
        final Status st1 = Status.newBuilder().setCode(Status.StatusCode.Aborted).setMessage("Some message").build();
        final Status st2 = Status.newBuilder().setMessage("error message").build();
        final Status st3 = Status.newBuilder().setCode(Status.StatusCode.AlreadyExists).build();
        final Status st4 = Status.newBuilder().build();

        assertThat(OTelProtoHelper.getSpanStatusAttributes(st1).size()==2).isTrue();
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoHelper.getSpanStatusAttributes(st1).get(OTelProtoHelper.STATUS_CODE)).equals(st1.getCode())).isTrue();
        assertThat(OTelProtoHelper.getSpanStatusAttributes(st1).get(OTelProtoHelper.STATUS_MESSAGE).equals(st1.getMessage())).isTrue();

        assertThat(OTelProtoHelper.getSpanStatusAttributes(st2).size()==2).isTrue();
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoHelper.getSpanStatusAttributes(st2).get(OTelProtoHelper.STATUS_CODE)).equals(st2.getCode())).isTrue();

        assertThat(OTelProtoHelper.getSpanStatusAttributes(st3).size()==1).isTrue();
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoHelper.getSpanStatusAttributes(st3).get(OTelProtoHelper.STATUS_CODE)).equals(st3.getCode())).isTrue();

        assertThat(OTelProtoHelper.getSpanStatusAttributes(st4).size()==1).isTrue();
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoHelper.getSpanStatusAttributes(st4).get(OTelProtoHelper.STATUS_CODE)).equals(st4.getCode())).isTrue();

    }

    @Test
    public void testISO8601(){
        final long NANO_MULTIPLIER = 1_000*1_000*1_000;
        final Span startTimeUnixNano = Span.newBuilder().setStartTimeUnixNano(651242400000000321L).build();
        final Span endTimeUnixNano = Span.newBuilder().setEndTimeUnixNano(1598013600000000321L).build();
        final Span emptyTimeSpan = Span.newBuilder().build();

        final String startTime = OTelProtoHelper.getStartTimeISO8601(startTimeUnixNano);
        assertThat(Instant.parse(startTime).getEpochSecond()*NANO_MULTIPLIER + Instant.parse(startTime).getNano() == startTimeUnixNano.getStartTimeUnixNano()).isTrue();
        final String endTime = OTelProtoHelper.getEndTimeISO8601(endTimeUnixNano);
        assertThat(Instant.parse(endTime).getEpochSecond()*NANO_MULTIPLIER + Instant.parse(endTime).getNano() == endTimeUnixNano.getEndTimeUnixNano()).isTrue();
        final String emptyTime = OTelProtoHelper.getStartTimeISO8601(endTimeUnixNano);
        assertThat(Instant.parse(emptyTime).getEpochSecond()*NANO_MULTIPLIER + Instant.parse(emptyTime).getNano() == emptyTimeSpan.getStartTimeUnixNano()).isTrue();

    }
}
