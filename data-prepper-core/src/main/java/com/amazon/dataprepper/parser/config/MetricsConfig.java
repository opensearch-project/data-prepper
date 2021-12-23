/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.amazon.dataprepper.pipeline.server.CloudWatchMeterRegistryProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.amazon.dataprepper.DataPrepper.getServiceNameForMetrics;
import static com.amazon.dataprepper.metrics.MetricNames.SERVICE_NAME;

@Configuration
public class MetricsConfig {
    @Bean
    public YAMLFactory yamlFactory() {
        return new YAMLFactory();
    }

    @Bean
    public ObjectMapper objectMapper(final YAMLFactory yamlFactory) {
        return new ObjectMapper(yamlFactory);
    }

    @Bean
    public ClassLoaderMetrics classLoaderMetrics() {
        return new ClassLoaderMetrics();
    }

    @Bean
    public JvmMemoryMetrics jvmMemoryMetrics() {
        return new JvmMemoryMetrics();
    }

    @Bean
    public JvmGcMetrics jvmGcMetrics() {
        return new JvmGcMetrics();
    }

    @Bean
    public ProcessorMetrics processorMetrics() {
        return new ProcessorMetrics();
    }

    @Bean
    public JvmThreadMetrics jvmThreadMetrics() {
        return new JvmThreadMetrics();
    }

    private void configureMetricRegistry(MeterRegistry meterRegistry) {
        meterRegistry.config()
                .commonTags(Arrays.asList(
                        Tag.of(SERVICE_NAME, getServiceNameForMetrics())
                ));
    }

    @Bean
    public Optional<PrometheusMeterRegistry> prometheusMeterRegistry(final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.Prometheus)) {
            PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            configureMetricRegistry(meterRegistry);
            return Optional.of(meterRegistry);
        }
        else {
            return Optional.empty();
        }
    }

    @Bean
    public CloudWatchMeterRegistryProvider cloudWatchMeterRegistryProvider() {
        return new CloudWatchMeterRegistryProvider();
    }

    @Bean
    public Optional<CloudWatchMeterRegistry> cloudWatchMeterRegistry(
            final DataPrepperConfiguration dataPrepperConfiguration,
            CloudWatchMeterRegistryProvider cloudWatchMeterRegistryProvider
    ) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.CloudWatch)) {
            CloudWatchMeterRegistry meterRegistry = cloudWatchMeterRegistryProvider.getCloudWatchMeterRegistry();
            configureMetricRegistry(meterRegistry);
            return Optional.of(meterRegistry);
        }
        else {
            return Optional.empty();
        }
    }

    @Bean
    public CompositeMeterRegistry systemMeterRegistry(
            final List<MeterBinder> meterBinders,
            final List<Optional<MeterRegistry>> optionalMeterRegistries,
            final DataPrepperConfiguration dataPrepperConfiguration
    ) {
        final CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();

        meterBinders.forEach(binder -> binder.bindTo(compositeMeterRegistry));

        optionalMeterRegistries.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(meterRegistry -> {
                    compositeMeterRegistry.add(meterRegistry);
                    Metrics.addRegistry(meterRegistry);
                });

        return compositeMeterRegistry;
    }
}
