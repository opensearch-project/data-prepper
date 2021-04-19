/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.metrics;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

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
