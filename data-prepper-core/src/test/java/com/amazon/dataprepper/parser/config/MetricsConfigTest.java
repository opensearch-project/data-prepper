/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.amazon.dataprepper.pipeline.server.PrometheusMetricsHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class MetricsConfigTest {
    private static final MetricsConfig metricsConfig = new MetricsConfig();

    @Test
    public void testYamlFactory() {
        final YAMLFactory factory = metricsConfig.yamlFactory();

        assertThat(factory, isA(YAMLFactory.class));
    }

    @Test
    public void testObjectMapper() {
        final ObjectMapper mapper = metricsConfig.objectMapper(null);

        assertThat(mapper, isA(ObjectMapper.class));
    }

    @Test
    public void testClassLoaderMetrics() {
        final ClassLoaderMetrics actual = metricsConfig.classLoaderMetrics();

        assertThat(actual, isA(ClassLoaderMetrics.class));
    }

    @Test
    public void testJvmMemoryMetrics() {
        final JvmMemoryMetrics actual = metricsConfig.jvmMemoryMetrics();

        assertThat(actual, isA(JvmMemoryMetrics.class));
    }

    @Test
    public void testJvmGcMetrics() {
        final JvmGcMetrics actual = metricsConfig.jvmGcMetrics();

        assertThat(actual, isA(JvmGcMetrics.class));
    }

    @Test
    public void testProcessorMetrics() {
        final ProcessorMetrics actual = metricsConfig.processorMetrics();

        assertThat(actual, isA(ProcessorMetrics.class));
    }

    @Test
    public void testJvmThreadMetrics() {
        final JvmThreadMetrics actual = metricsConfig.jvmThreadMetrics();

        assertThat(actual, isA(JvmThreadMetrics.class));
    }

    @Test
    public void testGivenConfigWithPrometheusMeterRegistryThenMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        final HttpServer server = mock(HttpServer.class);
        final HttpContext context = mock(HttpContext.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.Prometheus));
        when(server.createContext(anyString(), any(PrometheusMetricsHandler.class)))
                .thenReturn(context);

        final MeterRegistry meterRegistry = metricsConfig.prometheusMeterRegistry(
                dataPrepperConfiguration,
                null,
                server);

        assertThat(meterRegistry, isA(PrometheusMeterRegistry.class));

        verify(server, times(2))
                .createContext(anyString(), any(PrometheusMetricsHandler.class));
    }

    @Test
    public void testGivenConfigWithPrometheusMeterRegistryAndAuthenticatorThenMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final HttpServer server = mock(HttpServer.class);
        final HttpContext context = mock(HttpContext.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.Prometheus));
        when(server.createContext(anyString(), any(PrometheusMetricsHandler.class)))
                .thenReturn(context);

        final MeterRegistry meterRegistry = metricsConfig.prometheusMeterRegistry(
                dataPrepperConfiguration,
                authenticator,
                server);

        assertThat(meterRegistry, isA(PrometheusMeterRegistry.class));

        verify(server, times(2))
                .createContext(anyString(), any(PrometheusMetricsHandler.class));

        verify(context, times(2))
                .setAuthenticator(eq(authenticator));
    }

    @Test
    public void testGivenEmptyConfigThenMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.emptyList());

        final MeterRegistry meterRegistry = metricsConfig.prometheusMeterRegistry(
                dataPrepperConfiguration,
                null,
                null);

        assertThat(meterRegistry, is(nullValue()));
    }

    @Test
    public void testGivenConfigWithNoCloudWatchMeterRegistryThenNoMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.Prometheus));

        final MeterRegistry meterRegistry = metricsConfig.cloudWatchMeterRegistry(dataPrepperConfiguration);

        assertThat(meterRegistry, is(nullValue()));
    }

    @Test
    public void testGivenListOfMeterBindersWhenSystemMeterRegistryThenAllMeterBindersRegistered() {
        final Random r = new Random();
        final int copies = r.nextInt(10) + 5;
        final MeterBinder binder = mock(MeterBinder.class);
        final List<MeterBinder> meterBinders = Collections.nCopies(copies, binder);

        final MeterRegistry meterRegistryMock = mock(MeterRegistry.class);
        final List<MeterRegistry> meterRegistries = Collections.nCopies(1, meterRegistryMock);

        final CompositeMeterRegistry meterRegistry = metricsConfig.systemMeterRegistry(
                meterBinders,
                meterRegistries);

        assertThat(meterRegistry, isA(CompositeMeterRegistry.class));
        verify(binder, times(copies)).bindTo(any(MeterRegistry.class));
    }

    @Test
    public void testGivenEmptyListOfMeterBindersWhenSystemMeterRegistryThenNoMeterBindersRegistered() {
        final List<MeterBinder> meterBinders = Collections.emptyList();

        final MeterRegistry meterRegistryMock = mock(MeterRegistry.class);
        final List<MeterRegistry> meterRegistries = Collections.nCopies(1, meterRegistryMock);

        final CompositeMeterRegistry meterRegistry = metricsConfig.systemMeterRegistry(
                meterBinders,
                meterRegistries);

        assertThat(meterRegistry, isA(CompositeMeterRegistry.class));
    }
}