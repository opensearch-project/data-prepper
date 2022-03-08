/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics.model;

import com.amazon.dataprepper.plugins.processor.otelmetrics.OTelMetricsProtoHelper;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RawHistogramBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(RawHistogramBuilder.class);

    private String serviceName;
    private String unit;
    private String kind;
    private String name;
    private Map<String, Object> attributes;
    private String description;
    private String startTime;
    private String time;
    private int bucketsCount;
    private int explicitBoundsCount;
    private Long count;
    private List<RawHistogram.Bucket> buckets;
    private Double sum;


    public String getName() {
        return name;
    }

    public String getUnit() {
        return unit;
    }

    public String getDescription() {
        return description;
    }

    public String getKind() {
        return kind;
    }

    public String getTime() {
        return time;
    }

    public double getSum() {
        return sum;
    }

    public int getExplicitBoundsCount() {
        return explicitBoundsCount;
    }

    public int getBucketsCount() {
        return bucketsCount;
    }

    public Long getCount() {
        return count;
    }

    public List<RawHistogram.Bucket> getBuckets() {
        return buckets;
    }

    private RawHistogramBuilder setHistogramAttributes(final Map<String, Object> spanAttributes,
                                                       final Map<String, Object> resourceAttributes,
                                                       final Map<String, Object> instrumentationAttributes,
                                                       final List<RawHistogram.Bucket> buckets
    ) {
        this.attributes = new HashMap<>();
        this.attributes.putAll(spanAttributes);
        this.attributes.putAll(resourceAttributes);
        this.attributes.putAll(instrumentationAttributes);
        this.buckets = buckets;
        return this;
    }

    public RawHistogramBuilder setKind(String kind) {
        this.kind = kind;
        return this;
    }

    public RawHistogramBuilder setUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public RawHistogramBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public RawHistogramBuilder setDescription(String description) {
        this.description = description;
        return this;
    }

    public RawHistogramBuilder setStartTime(String startTime) {
        this.startTime = startTime;
        return this;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public RawHistogramBuilder setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public RawHistogramBuilder setTime(String time) {
        this.time = time;
        return this;
    }

    public RawHistogramBuilder setBucketsCount(int bucketsCount) {
        this.bucketsCount = bucketsCount;
        return this;
    }

    private RawHistogramBuilder setExplicitBoundsCount(int explicitBoundsCount) {
        this.explicitBoundsCount = explicitBoundsCount;
        return this;
    }

    private RawHistogramBuilder setCount(Long count) {
        this.count = count;
        return this;
    }

    public RawHistogramBuilder setFromHistogram(Metric metric, InstrumentationLibrary instrumentationLibrary, String serviceName, Map<String, Object> resourceAttributes, HistogramDataPoint dp) {
        return this
                .setKind("histogram")
                .setUnit(metric.getUnit())
                .setName(metric.getName())
                .setDescription(metric.getDescription())
                .setBucketsCount(dp.getBucketCountsCount())
                .setExplicitBoundsCount(dp.getExplicitBoundsCount())
                .setStartTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                .setTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                .setServiceName(serviceName)
                .setCount(dp.getCount())
                .setSum(dp.getSum())
                .setHistogramAttributes(
                        OTelMetricsProtoHelper.unpackKeyValueList(dp.getAttributesList()),
                        resourceAttributes,
                        OTelMetricsProtoHelper.getInstrumentationLibraryAttributes(instrumentationLibrary),
                        createBuckets(dp.getBucketCountsList(), dp.getExplicitBoundsList())
                );

    }

    private RawHistogramBuilder setSum(double sum) {
        this.sum = sum;
        return this;
    }

    public static List<RawHistogram.Bucket> createBuckets(List<Long> bucketCountsList, List<Double> explicitBoundsList) {
        List<RawHistogram.Bucket> buckets = new ArrayList<>();
        if (bucketCountsList.size() - 1 != explicitBoundsList.size()) {
            LOG.error("bucket count list not equals to bounds list {} {}", bucketCountsList.size(), explicitBoundsList.size() );
        } else {
            double previousBound = 0.0;
            for (int i = 0; i < bucketCountsList.size(); i++) {
                Long bucketCount = bucketCountsList.get(i);
                double bound = i == 0 ? previousBound : explicitBoundsList.get(i - 1);
                buckets.add(new RawHistogram.Bucket(previousBound, bound, bucketCount));
                previousBound = bound;
            }
        }
        return buckets;
    }


    public RawHistogram build() {
        return new RawHistogram(this);
    }
}
