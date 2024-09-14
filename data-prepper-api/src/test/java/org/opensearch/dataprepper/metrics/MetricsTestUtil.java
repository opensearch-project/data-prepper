/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.metrics;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MetricsTestUtil {

    public static synchronized void initMetrics() {
        final Set<MeterRegistry> registries = new HashSet<>(Metrics.globalRegistry.getRegistries());
        registries.forEach(Metrics.globalRegistry::remove);

        final List<Meter> meters = new ArrayList<>(Metrics.globalRegistry.getMeters());
        meters.forEach(Metrics.globalRegistry::remove);

        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    public static synchronized List<Measurement> getMeasurementList(final String meterName) {
        final Meter meter = getRegistry().find(meterName).meter();
        if(meter == null)
            throw new RuntimeException("No metrics meter is available for " + meterName);

        return StreamSupport.stream(meter.measure().spliterator(), false)
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
