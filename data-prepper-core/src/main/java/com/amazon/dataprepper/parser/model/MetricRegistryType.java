package com.amazon.dataprepper.parser.model;

import com.amazon.dataprepper.pipeline.server.CloudWatchMeterRegistryProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import static java.lang.String.format;

public enum MetricRegistryType {
    Prometheus,
    CloudWatch;

    public static MeterRegistry getDefaultMeterRegistryForType(final MetricRegistryType metricRegistryType) {
        switch (metricRegistryType) {
            case Prometheus:
                return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            case CloudWatch:
                return new CloudWatchMeterRegistryProvider().getCloudWatchMeterRegistry();
            default:
                throw new IllegalArgumentException(format("Invalid metricRegistryType %s", metricRegistryType));

        }
    }
}