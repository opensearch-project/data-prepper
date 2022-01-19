/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.model;

import com.amazon.dataprepper.pipeline.server.CloudWatchMeterRegistryProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.Arrays;

import static com.amazon.dataprepper.core.DataPrepper.getServiceNameForMetrics;
import static com.amazon.dataprepper.metrics.MetricNames.SERVICE_NAME;
import static java.lang.String.format;

public enum MetricRegistryType {
    Prometheus,
    CloudWatch;

    public static MeterRegistry getDefaultMeterRegistryForType(final MetricRegistryType metricRegistryType) {
        MeterRegistry meterRegistry = null;
        switch (metricRegistryType) {
            case Prometheus:
                meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
                break;
            case CloudWatch:
                meterRegistry = new CloudWatchMeterRegistryProvider().getCloudWatchMeterRegistry();
                break;
            default:
                throw new IllegalArgumentException(format("Invalid metricRegistryType %s", metricRegistryType));
        }
        meterRegistry.config().commonTags(Arrays.asList(Tag.of(SERVICE_NAME, getServiceNameForMetrics())));
        return meterRegistry;
    }
}