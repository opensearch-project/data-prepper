package com.amazon.dataprepper.integration;

import io.opentelemetry.proto.trace.v1.Span;

public enum EndToEndTestData {

    DATA_100("TRACE_1", "100", "ServiceA", null, "FRUITS", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_200("TRACE_1", "200", "ServiceA", "100", "CALL_SERVICE_B_APPLE", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_300("TRACE_1", "300", "ServiceB", "200", "/APPLE", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_400("TRACE_1", "400", "ServiceB", "300", "CALL_SERVICE_C_ORANGE", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_500("TRACE_1", "500", "ServiceC", "400", "/ORANGE", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_600("TRACE_1", "600", "ServiceC", "500", "SOME_INTERNAL", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_700("TRACE_1", "700", "ServiceA", "100", "CALL_SERVICE_B_JACKFRUIT", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_800("TRACE_1", "800", "ServiceB", "700", "/JACKFRUIT", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_900("TRACE_1", "900", "ServiceB", "800", "SOME_INTERNAL", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_1000("TRACE_1", "1000", "ServiceA", "100", "CALL_SERVICE_D_PEAR", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_1100("TRACE_1", "1100", "ServiceD", "1000", "/PEAR", Span.SpanKind.SPAN_KIND_SERVER),

    DATA_101("TRACE_2", "101", "ServiceA", null, "VEGGIES", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_201("TRACE_2", "201", "ServiceA", "101", "CALL_SERVICE_B_ONION", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_301("TRACE_2", "301", "ServiceB", "201", "/ONION", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_401("TRACE_2", "401", "ServiceA", "101", "CALL_SERVICE_E_POTATO", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_501("TRACE_2", "501", "ServiceE", "401", "/POTATO", Span.SpanKind.SPAN_KIND_SERVER);

    public final String traceId;
    public final String spanId;
    public final String serviceName;
    public final String parentId;
    public final String name;
    public final Span.SpanKind spanKind;

    EndToEndTestData(String traceId, String spanId, String serviceName, String parentId, String name, Span.SpanKind kind) {
        this.traceId = traceId;
        this.spanId = spanId;
        this.serviceName = serviceName;
        this.parentId = parentId;
        this.name = name;
        this.spanKind = kind;
    }
}
