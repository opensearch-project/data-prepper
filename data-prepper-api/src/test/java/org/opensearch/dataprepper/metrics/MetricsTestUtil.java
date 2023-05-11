/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.metrics;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MetricsTestUtil {

    public static void initMetrics() {
        Metrics.globalRegistry.getRegistries().forEach(meterRegistry -> Metrics.globalRegistry.remove(meterRegistry));
        Metrics.globalRegistry.getMeters().forEach(meter -> Metrics.globalRegistry.remove(meter));
        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    public static List<Measurement> getMeasurementList(final String meterName) {
        return StreamSupport.stream(getRegistry().find(meterName).meter().measure().spliterator(), false)
                .collect(Collectors.toList());
    }

    public static Measurement getMeasurementFromList(final List<Measurement> measurements, final Statistic statistic) {
        return measurements.stream().filter(measurement -> measurement.getStatistic() == statistic).findAny().get();
    }

    private static MeterRegistry getRegistry() {
        return Metrics.globalRegistry.getRegistries().iterator().next();
    }

    public static boolean isBetween(double value, double low, double high) {
        return value > low && value < high;
    }

}
