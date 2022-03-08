/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

import java.util.List;
import java.util.Map;

public class RawSummary {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Map<String, Object> attributes;
    private final String kind;
    private final String unit;
    private final String name;
    private final String description;
    private final String startTime;
    private final String time;
    private final String serviceName;
    private final int quantileValuesCount;
    private final List<SummaryDataPoint.ValueAtQuantile> quantileValuesList;

    public String getKind() {
        return kind;
    }

    public RawSummary(RawSummaryBuilder builder) {
        this.attributes = builder.getAttributes();
        this.kind = builder.getKind();
        this.unit = builder.getUnit();
        this.name = builder.getName();
        this.description = builder.getDescription();
        this.startTime = builder.getStartTime();
        this.time = builder.getTime();
        this.serviceName = builder.getServiceName();
        this.quantileValuesCount = builder.getQuantileValuesCount();
        this.quantileValuesList = builder.getQuantileValuesList();
    }

    @JsonAnyGetter
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public int getQuantileValuesCount() {
        return quantileValuesCount;
    }

    public List<SummaryDataPoint.ValueAtQuantile> getQuantileValuesList() {
        return quantileValuesList;
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getTime() {
        return time;
    }

    public String getUnit() {
        return unit;
    }

    public String toJson() throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }
}
