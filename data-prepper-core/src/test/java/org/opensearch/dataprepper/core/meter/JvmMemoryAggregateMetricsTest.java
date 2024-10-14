/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.meter;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.mockito.Mockito.mockStatic;
import static org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics.AREA_TAG_NAME;
import static org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics.JVM_MEMORY_USED;

@ExtendWith(MockitoExtension.class)
class JvmMemoryAggregateMetricsTest {
    @Mock
    private MemoryMXBean memoryMXBean;

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    private JvmMemoryAggregateMetrics createObjectUnderTest() {
        try (final MockedStatic<ManagementFactory> managementFactoryMockedStatic = mockStatic(ManagementFactory.class)) {
            managementFactoryMockedStatic.when(ManagementFactory::getMemoryMXBean).thenReturn(memoryMXBean);
            return new JvmMemoryAggregateMetrics();
        }
    }

    @Test
    void bindTo_registers_heap_Meter_with_MeterRegistry() {
        createObjectUnderTest().bindTo(meterRegistry);

        assertThat(meterRegistry.getMeters().size(), equalTo(2));

        final Meter heapMeter = meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getTag(AREA_TAG_NAME).equals("heap"))
                .findFirst()
                .orElse(null);

        assertThat(heapMeter, notNullValue());
        assertThat(heapMeter.getId().getName(), equalTo(JVM_MEMORY_USED));
        assertThat(heapMeter.getId().getBaseUnit(), equalTo(BaseUnits.BYTES));
        assertThat(heapMeter.getId().getDescription(), allOf(notNullValue(), not(emptyString())));
    }

    @Test
    void bindTo_registers_nonheap_Meter_with_MeterRegistry() {
        createObjectUnderTest().bindTo(meterRegistry);

        assertThat(meterRegistry.getMeters().size(), equalTo(2));

        final Meter nonHeapMeter = meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getTag(AREA_TAG_NAME).equals("nonheap"))
                .findFirst()
                .orElse(null);

        assertThat(nonHeapMeter, notNullValue());
        assertThat(nonHeapMeter.getId().getName(), equalTo(JVM_MEMORY_USED));
        assertThat(nonHeapMeter.getId().getBaseUnit(), equalTo(BaseUnits.BYTES));
        assertThat(nonHeapMeter.getId().getDescription(), allOf(notNullValue(), not(emptyString())));
    }
}