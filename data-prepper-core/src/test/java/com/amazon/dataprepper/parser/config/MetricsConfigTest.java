/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class MetricsConfigTest {
    private static final MetricsConfig metricsConfig = new MetricsConfig();

    @Test
    public void testYamlFactory() {
        YAMLFactory factory = metricsConfig.yamlFactory();

        assertThat(factory, isA(YAMLFactory.class));
    }

    @Test
    public void testObjectMapper() {
        ObjectMapper mapper = metricsConfig.objectMapper(null);

        assertThat(mapper, isA(ObjectMapper.class));
    }

    @Test
    public void testClassLoaderMetrics() {
        ClassLoaderMetrics actual = metricsConfig.classLoaderMetrics();

        assertThat(actual, isA(ClassLoaderMetrics.class));
    }

    @Test
    public void testJvmMemoryMetrics() {
        JvmMemoryMetrics actual = metricsConfig.jvmMemoryMetrics();

        assertThat(actual, isA(JvmMemoryMetrics.class));
    }

    @Test
    public void testJvmGcMetrics() {
        JvmGcMetrics actual = metricsConfig.jvmGcMetrics();

        assertThat(actual, isA(JvmGcMetrics.class));
    }

    @Test
    public void testProcessorMetrics() {
        ProcessorMetrics actual = metricsConfig.processorMetrics();

        assertThat(actual, isA(ProcessorMetrics.class));
    }

    @Test
    public void testJvmThreadMetrics() {
        JvmThreadMetrics actual = metricsConfig.jvmThreadMetrics();

        assertThat(actual, isA(JvmThreadMetrics.class));
    }

    @Test
    public void testGivenListOfMeterBindersWhenSystemMeterRegistryThenAllMeterBindersRegistered() {
        Random r = new Random();
        int copies = r.nextInt(10) + 5;
        MeterBinder binder = mock(MeterBinder.class);
        List<MeterBinder> meterBinders = Collections.nCopies(copies, binder);
        DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        MetricRegistryType registryType = mock(MetricRegistryType.class);
        List<MetricRegistryType> registryTypes = Collections.nCopies(copies, registryType);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(registryTypes);

        CompositeMeterRegistry meterRegistry = metricsConfig.systemMeterRegistry(meterBinders, dataPrepperConfiguration);

        assertThat(meterRegistry, isA(CompositeMeterRegistry.class));
        verify(binder, times(copies)).bindTo(any(MeterRegistry.class));
    }

    @Test
    public void testGivenEmptyListOfMeterBindersWhenSystemMeterRegistryThenNoMeterBindersRegistered() {
        List<MeterBinder> meterBinders = Collections.emptyList();
        DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        List<MetricRegistryType> registryTypes = Collections.emptyList();

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(registryTypes);

        CompositeMeterRegistry meterRegistry = metricsConfig.systemMeterRegistry(meterBinders, dataPrepperConfiguration);

        assertThat(meterRegistry, isA(CompositeMeterRegistry.class));
    }
}