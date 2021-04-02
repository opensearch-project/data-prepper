package com.amazon.dataprepper.integration;

import io.opentelemetry.proto.trace.v1.Span;

public enum EndToEndTestData {

    DATA_100("100", "ServiceA", null, "FRUITS", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_200("200", "ServiceA", "100", "CALL_SERVICE_B_APPLE", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_300("300", "ServiceB", "200", "/APPLE", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_400("400", "ServiceB", "300", "CALL_SERVICE_C_ORANGE", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_500("500", "ServiceC", "400", "/ORANGE", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_600("600", "ServiceC", "500", "SOME_INTERNAL", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_700("700", "ServiceA", "100", "CALL_SERVICE_B_JACKFRUIT", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_800("800", "ServiceB", "700", "/JACKFRUIT", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_900("900", "ServiceB", "800", "SOME_INTERNAL", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_1000("1000", "ServiceA", "100", "CALL_SERVICE_D_PEAR", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_1100("1100", "ServiceD", "1000", "/PEAR", Span.SpanKind.SPAN_KIND_SERVER),

    DATA_101("101", "ServiceA", null, "VEGGIES", Span.SpanKind.SPAN_KIND_INTERNAL),
    DATA_201("201", "ServiceA", "101", "CALL_SERVICE_B_ONION", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_301("301", "ServiceB", "201", "/ONION", Span.SpanKind.SPAN_KIND_SERVER),
    DATA_401("401", "ServiceA", "101", "CALL_SERVICE_E_POTATO", Span.SpanKind.SPAN_KIND_CLIENT),
    DATA_501("501", "ServiceE", "401", "/POTATO", Span.SpanKind.SPAN_KIND_SERVER);

    public final String spanId;
    public final String serviceName;
    public final String parentId;
    public final String name;
    public final Span.SpanKind spanKind;

    EndToEndTestData(String spanId, String serviceName, String parentId, String name, Span.SpanKind kind) {
        this.spanId = spanId;
        this.serviceName = serviceName;
        this.parentId = parentId;
        this.name = name;
        this.spanKind = kind;
    }
}
