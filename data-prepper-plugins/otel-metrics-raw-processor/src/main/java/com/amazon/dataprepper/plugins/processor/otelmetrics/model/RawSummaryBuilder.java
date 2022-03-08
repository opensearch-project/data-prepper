/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics.model;

import com.amazon.dataprepper.plugins.processor.otelmetrics.OTelMetricsProtoHelper;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RawSummaryBuilder {

    private String kind;
    private String startTime;
    private String time;
    private String value;
    private String unit;
    private String serviceName;
    private String name;
    private String description;
    private HashMap<String, Object> attributes;
    private int quantileValuesCount;
    private List<SummaryDataPoint.ValueAtQuantile> quantileValuesList;


    public int getQuantileValuesCount() {
        return quantileValuesCount;
    }

    public List<SummaryDataPoint.ValueAtQuantile> getQuantileValuesList() {
        return quantileValuesList;
    }

    public String getKind() {
        return kind;
    }
    public String getStartTime() {
        return startTime;
    }

    public String getServiceName() {
        return serviceName;
    }


    public RawSummaryBuilder setKind(String kind) {
        this.kind = kind;
        return this;
    }

    public RawSummaryBuilder setValue(String value) {
        this.value = value;
        return this;
    }

    public String getValue() {
        return value;
    }

    public String getTime() {
        return time;
    }

    public RawSummaryBuilder setStartTime(String startTime) {
        this.startTime = startTime;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public RawSummaryBuilder setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public RawSummaryBuilder setTime(String time) {
        this.time = time;
        return this;
    }

    public RawSummaryBuilder setUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public String getUnit() {
        return unit;
    }

    public RawSummaryBuilder setDescription(String description) {
        this.description = description;
        return this;
    }

    public RawSummaryBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }

    public RawSummaryBuilder setFromSummary(Metric metric, InstrumentationLibrary instrumentationLibrary, String serviceName, Map<String, Object> resourceAttributes, SummaryDataPoint dp) {
        return this
                .setKind("summary")
                .setUnit(metric.getUnit())
                .setName(metric.getName())
                .setDescription(metric.getDescription())
                .setStartTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                .setTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                .setServiceName(serviceName)
                .setQuantileValues(dp.getQuantileValuesList())
                .setQuantileValuesCount(dp.getQuantileValuesCount())
                .setSummaryAttributes(OTelMetricsProtoHelper.unpackKeyValueList(dp.getAttributesList()),
                        resourceAttributes,
                        OTelMetricsProtoHelper.getInstrumentationLibraryAttributes(instrumentationLibrary));
    }

    private RawSummaryBuilder setQuantileValuesCount(int quantileValuesCount) {
        this.quantileValuesCount = quantileValuesCount;
        return this;
    }

    private RawSummaryBuilder setQuantileValues(List<SummaryDataPoint.ValueAtQuantile> quantileValuesList) {
        this.quantileValuesList = quantileValuesList;
        return this;
    }

    public RawSummary build() {
        return new RawSummary(this);
    }

    private RawSummaryBuilder setSummaryAttributes(final Map<String, Object> spanAttributes,
                                           final Map<String, Object> resourceAttributes,
                                           final Map<String, Object> instrumentationAttributes
    ) {
        this.attributes = new HashMap<>();
        this.attributes.putAll(spanAttributes);
        this.attributes.putAll(resourceAttributes);
        this.attributes.putAll(instrumentationAttributes);

        return this;
    }
}
