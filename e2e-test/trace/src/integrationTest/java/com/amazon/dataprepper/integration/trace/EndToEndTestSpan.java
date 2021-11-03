package com.amazon.dataprepper.integration.trace;

import io.opentelemetry.proto.trace.v1.Span;

public enum EndToEndTestSpan {

    TRACE_1_ROOT_SPAN("TRACE_1", "100", "ServiceA", null, "FRUITS", Span.SpanKind.SPAN_KIND_INTERNAL,
            "2020-08-20T05:40:46.089556800Z", 48545200L, 1),
    TRACE_1_SPAN_2("TRACE_1", "200", "ServiceA", "100", "CALL_SERVICE_B_APPLE", Span.SpanKind.SPAN_KIND_CLIENT,
            "2020-08-20T05:40:46.089556799Z", 47545200L, 0),
    TRACE_1_SPAN_3("TRACE_1", "300", "ServiceB", "200", "/APPLE", Span.SpanKind.SPAN_KIND_SERVER,
            "2020-08-20T05:40:46.089556798Z", 46545200L, 0),
    TRACE_1_SPAN_4("TRACE_1", "400", "ServiceB", "300", "CALL_SERVICE_C_ORANGE", Span.SpanKind.SPAN_KIND_CLIENT,
            "2020-08-20T05:40:46.089556797Z", 45545200L, 0),
    TRACE_1_SPAN_5("TRACE_1", "500", "ServiceC", "400", "/ORANGE", Span.SpanKind.SPAN_KIND_SERVER,
            "2020-08-20T05:40:46.089556796Z", 44545200L, 0),
    TRACE_1_SPAN_6("TRACE_1", "600", "ServiceC", "500", "SOME_INTERNAL", Span.SpanKind.SPAN_KIND_INTERNAL,
            "2020-08-20T05:40:46.089556795Z", 43545200L, 0),
    TRACE_1_SPAN_7("TRACE_1", "700", "ServiceA", "100", "CALL_SERVICE_B_JACKFRUIT", Span.SpanKind.SPAN_KIND_CLIENT,
            "2020-08-20T05:40:46.089556794Z", 42545200L, 0),
    TRACE_1_SPAN_8("TRACE_1", "800", "ServiceB", "700", "/JACKFRUIT", Span.SpanKind.SPAN_KIND_SERVER,
            "2020-08-20T05:40:46.089556793Z", 41545200L, 0),
    TRACE_1_SPAN_9("TRACE_1", "900", "ServiceB", "800", "SOME_INTERNAL", Span.SpanKind.SPAN_KIND_INTERNAL,
            "2020-08-20T05:40:46.089556792Z", 40545200L, 0),
    TRACE_1_SPAN_10("TRACE_1", "1000", "ServiceA", "100", "CALL_SERVICE_D_PEAR", Span.SpanKind.SPAN_KIND_CLIENT,
            "2020-08-20T05:40:46.089556791Z", 39545200L, 0),
    TRACE_1_SPAN_11("TRACE_1", "1100", "ServiceD", "1000", "/PEAR", Span.SpanKind.SPAN_KIND_SERVER,
            "2020-08-20T05:40:46.089556790Z", 38545200L, 0),

    TRACE_2_ROOT_SPAN("TRACE_2", "101", "ServiceA", null, "VEGGIES", Span.SpanKind.SPAN_KIND_INTERNAL,
            "2020-08-20T05:40:43.217170200Z", 49160000L, 1),
    TRACE_2_SPAN_2("TRACE_2", "201", "ServiceA", "101", "CALL_SERVICE_B_ONION", Span.SpanKind.SPAN_KIND_CLIENT,
            "2020-08-20T05:40:43.217170199Z", 48160000L, 0),
    TRACE_2_SPAN_3("TRACE_2", "301", "ServiceB", "201", "/ONION", Span.SpanKind.SPAN_KIND_SERVER,
            "2020-08-20T05:40:43.217170198Z", 47160000L, 0),
    TRACE_2_SPAN_4("TRACE_2", "401", "ServiceA", "101", "CALL_SERVICE_E_POTATO", Span.SpanKind.SPAN_KIND_CLIENT,
            "2020-08-20T05:40:43.217170197Z", 46160000L, 0),
    TRACE_2_SPAN_5("TRACE_2", "501", "ServiceE", "401", "/POTATO", Span.SpanKind.SPAN_KIND_SERVER,
            "2020-08-20T05:40:43.217170196Z", 45160000L, 0);

    public final String traceId;
    public final String spanId;
    public final String serviceName;
    public final String parentId;
    public final String name;
    public final Span.SpanKind spanKind;
    public final String endTime;
    public final Long durationInNanos;
    public final Integer statusCode;

    EndToEndTestSpan(String traceId, String spanId, String serviceName, String parentId, String name, Span.SpanKind kind,
                     String endTime, Long durationInNanos, Integer statusCode) {
        this.traceId = traceId;
        this.spanId = spanId;
        this.serviceName = serviceName;
        this.parentId = parentId;
        this.name = name;
        this.spanKind = kind;
        this.endTime = endTime;
        this.durationInNanos = durationInNanos;
        this.statusCode = statusCode;
    }
}
