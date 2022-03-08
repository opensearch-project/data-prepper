/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public class RawHistogram {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Map<String, Object> attributes;

    private final String startTime;

    private final String time;

    private final String kind;

    private final String unit;

    private final String description;

    private final String name;

    private final String serviceName;

    private final Long count;

    private final List<Bucket> buckets;

    private final Double sum;

    @JsonAnyGetter
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getTime() {
        return time;
    }

    public String getKind() {
        return kind;
    }

    public String getUnit() {
        return unit;
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

    public Long getCount() {
        return count;
    }

    public Double getSum() {
        return sum;
    }

    @JsonProperty("values")
    public List<Bucket> getBuckets() {
        return buckets;
    }

    public RawHistogram(RawHistogramBuilder b) {
        this.time = b.getTime();
        this.kind = b.getKind();
        this.name = b.getName();
        this.serviceName = b.getServiceName();
        this.description = b.getDescription();
        this.unit = b.getUnit();
        this.attributes = b.getAttributes();
        this.startTime = b.getStartTime();
        this.count = b.getCount();
        this.buckets = b.getBuckets();
        this.sum = b.getSum();
    }

    public String toJson() throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    public static class Bucket {

        private final double lowerBound;
        private final double upperBound;
        private final long numberOfObservations;

        public Bucket(double lowerBound, double upperBound, long numberOfObservations) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.numberOfObservations = numberOfObservations;
        }

        @JsonProperty("lo")
        public double getLowerBound() {
            return lowerBound;
        }

        @JsonProperty("hi")
        public double getUpperBound() {
            return upperBound;
        }

        @JsonProperty("cnt")
        public long getNumberOfObservations() {
            return numberOfObservations;
        }

    }

}
