/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics.model;

import com.amazon.dataprepper.plugins.processor.otelmetrics.OTelMetricsProtoHelper;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import java.util.HashMap;
import java.util.Map;

public class RawGaugeBuilder {

    private String kind;
    private String startTime;
    private String time;
    private String value;
    private String unit;
    private String serviceName;
    private String name;
    private String description;

    private HashMap<String, Object> attributes;

    public String getKind() {
        return kind;
    }

    public RawGaugeBuilder setKind(String kind) {
        this.kind = kind;
        return this;
    }

    public RawGaugeBuilder setValue(String value) {
        this.value = value;
        return this;
    }

    public String getValue() {
        return value;
    }

    public String getTime() {
        return time;
    }

    public RawGaugeBuilder setStartTime(String startTime) {
        this.startTime = startTime;
        return this;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getServiceName() {
        return serviceName;
    }

    public HashMap<String, Object> getAttributes() {
        return attributes;
    }

    public RawGaugeBuilder setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public RawGaugeBuilder setTime(String time) {
        this.time = time;
        return this;
    }

    public RawGaugeBuilder setUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public String getUnit() {
        return unit;
    }

    public RawGaugeBuilder setDescription(String description) {
        this.description = description;
        return this;
    }

    public RawGaugeBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }

    private RawGaugeBuilder setGaugeAttributes(final Map<String, Object> spanAttributes,
                                               final Map<String, Object> resourceAttributes,
                                               final Map<String, Object> instrumentationAttributes
                                            ) {
        this.attributes = new HashMap<>();
        this.attributes.putAll(spanAttributes);
        this.attributes.putAll(resourceAttributes);
        this.attributes.putAll(instrumentationAttributes);

        return this;
    }

    public RawGaugeBuilder setFromGauge(Metric metric, InstrumentationLibrary instrumentationLibrary, String serviceName, Map<String, Object> resourceAttributes, NumberDataPoint dp) {

        return this
                .setKind("gauge")
                .setUnit(metric.getUnit())
                .setName(metric.getName())
                .setDescription(metric.getDescription())
                .setStartTime(OTelMetricsProtoHelper.getStartTimeISO8601(dp))
                .setTime(OTelMetricsProtoHelper.getTimeISO8601(dp))
                .setServiceName(serviceName)
                .setValue(OTelMetricsProtoHelper.getAsStringValue(dp))
                .setGaugeAttributes(OTelMetricsProtoHelper.convertKeysOfDataPointAttributes(dp),
                        resourceAttributes,
                        OTelMetricsProtoHelper.getInstrumentationLibraryAttributes(instrumentationLibrary));


    }

    public RawGauge build() {
        return new RawGauge(this);
    }
}
