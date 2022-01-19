/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.commons.codec.binary.Hex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public final class RawSpanBuilder {
    private static final String SERVICE_NAME = "service.name";

    String traceId;
    String spanId;
    String traceState;
    String parentSpanId;
    String name;
    String kind;
    String startTime;
    String endTime;
    long durationInNanos;
    String serviceName;
    Map<String, Object> attributes;
    List<RawEvent> events;
    List<RawLink> links;
    int droppedAttributesCount;
    int droppedEventsCount;
    int droppedLinksCount;
    TraceGroup traceGroup;


    public RawSpanBuilder() {
    }

    private RawSpanBuilder setTraceId(final String traceId) {
        this.traceId = traceId;
        return this;
    }

    private RawSpanBuilder setSpanId(final String spanId) {
        this.spanId = spanId;
        return this;
    }

    private RawSpanBuilder setTraceState(final String traceState) {
        this.traceState = traceState;
        return this;
    }

    private RawSpanBuilder setParentSpanId(final String parentSpanId) {
        this.parentSpanId = parentSpanId;
        return this;
    }

    private RawSpanBuilder setName(final String name) {
        this.name = name;
        return this;
    }

    private RawSpanBuilder setKind(final String kind) {
        this.kind = kind;
        return this;
    }

    private RawSpanBuilder setStartTime(final String startTime) {
        this.startTime = startTime;
        return this;
    }

    private RawSpanBuilder setEndTime(final String endTime) {
        this.endTime = endTime;
        return this;
    }

    private RawSpanBuilder setDurationInNanos(final long durationInNanos) {
        this.durationInNanos = durationInNanos;
        return this;
    }

    private RawSpanBuilder setServiceName(final String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    private RawSpanBuilder setDroppedAttributesCount(final int droppedAttributesCount) {
        this.droppedAttributesCount = droppedAttributesCount;
        return this;
    }

    private RawSpanBuilder setDroppedEventsCount(final int droppedEventsCount) {
        this.droppedEventsCount = droppedEventsCount;
        return this;
    }

    private RawSpanBuilder setDroppedLinksCount(final int droppedLinksCount) {
        this.droppedLinksCount = droppedLinksCount;
        return this;
    }

    private RawSpanBuilder setTraceGroup(final TraceGroup traceGroup) {
        this.traceGroup = traceGroup;
        return this;
    }


    private RawSpanBuilder setSpanAttributes(final Map<String, Object> spanAttributes,
                                             final Map<String, Object> resourceAttributes,
                                             final Map<String, Object> instrumentationAttributes,
                                             final Map<String, Object> statusAttributes) {
        this.attributes = new HashMap<>();
        this.attributes.putAll(spanAttributes);
        this.attributes.putAll(resourceAttributes);
        this.attributes.putAll(instrumentationAttributes);
        this.attributes.putAll(statusAttributes);
        return this;
    }

    private RawSpanBuilder setEvents(final List<RawEvent> events) {
        this.events = events;
        return this;
    }

    private RawSpanBuilder setLinks(final List<RawLink> links) {
        this.links = links;
        return this;
    }

    public RawSpan build() {
        return new RawSpan(this);
    }


    public RawSpanBuilder setFromSpan(final Span span, final InstrumentationLibrary instrumentationLibrary, final String serviceName, final Map<String, Object> resourceAttributes) {
        return this
                .setTraceId(Hex.encodeHexString(span.getTraceId().toByteArray()))
                .setSpanId(Hex.encodeHexString(span.getSpanId().toByteArray()))
                .setTraceState(span.getTraceState())
                .setParentSpanId(Hex.encodeHexString(span.getParentSpanId().toByteArray()))
                .setName(span.getName())
                .setKind(span.getKind().name())
                .setStartTime(OTelProtoHelper.getStartTimeISO8601(span))
                .setEndTime(OTelProtoHelper.getEndTimeISO8601(span))
                .setDurationInNanos(span.getEndTimeUnixNano() - span.getStartTimeUnixNano())
                .setServiceName(serviceName)
                .setSpanAttributes(OTelProtoHelper.getSpanAttributes(span),
                        resourceAttributes,
                        OTelProtoHelper.getInstrumentationLibraryAttributes(instrumentationLibrary),
                        OTelProtoHelper.getSpanStatusAttributes(span.getStatus()))
                .setEvents(span.getEventsList().stream().map(RawEvent::buildRawEvent).collect(Collectors.toList()))
                .setLinks(span.getLinksList().stream().map(RawLink::buildRawLink).collect(Collectors.toList()))
                .setDroppedAttributesCount(span.getDroppedAttributesCount())
                .setDroppedEventsCount(span.getDroppedEventsCount())
                .setDroppedLinksCount(span.getDroppedLinksCount())
                .setTraceGroup(OTelProtoHelper.getTraceGroup(span));
    }
}
