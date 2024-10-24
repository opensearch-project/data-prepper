/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.meter;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * Provides JVM metrics
 */
public class JvmMemoryAggregateMetrics implements MeterBinder {
    static final String AREA_TAG_NAME = "area";
    static final String JVM_MEMORY_USED = "jvm.memory.used";
    private final MemoryMXBean memoryMXBean;

    public JvmMemoryAggregateMetrics() {
        memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    @Override
    public void bindTo(final MeterRegistry registry) {
        final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        Gauge.builder(JVM_MEMORY_USED, memoryMXBean, (mem) -> mem.getHeapMemoryUsage().getUsed())
                .tags(Tags.of(Tag.of(AREA_TAG_NAME, "heap")))
                .description("The amount of used heap memory")
                .baseUnit(BaseUnits.BYTES)
                .register(registry);
        Gauge.builder(JVM_MEMORY_USED, memoryMXBean, (mem) -> mem.getNonHeapMemoryUsage().getUsed())
                .tags(Tags.of(Tag.of(AREA_TAG_NAME, "nonheap")))
                .description("The amount of used non-heap memory")
                .baseUnit(BaseUnits.BYTES)
                .register(registry);
    }
}
