/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.amazon.dataprepper.pipeline.server.CloudWatchMeterRegistryProvider;
import com.amazon.dataprepper.pipeline.server.PrometheusMetricsHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amazon.dataprepper.DataPrepper.getServiceNameForMetrics;
import static com.amazon.dataprepper.metrics.MetricNames.SERVICE_NAME;
import static java.lang.String.format;

@Configuration
public class MetricsConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);
    private static final String METRICS_CONTEXT_PREFIX = "/metrics";

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

    private void configureMetricRegistry(final MeterRegistry meterRegistry) {
        meterRegistry.config()
                .commonTags(Collections.singletonList(
                        Tag.of(SERVICE_NAME, getServiceNameForMetrics())
                ));

    }

    @Bean
    public Optional<MeterRegistry> prometheusMeterRegistry(
            final DataPrepperConfiguration dataPrepperConfiguration,
            final Optional<Authenticator> optionalAuthenticator,
            final HttpServer server
    ) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.Prometheus)) {
            final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            configureMetricRegistry(meterRegistry);

            final PrometheusMetricsHandler metricsHandler = new PrometheusMetricsHandler(meterRegistry);

            final List<HttpContext> contextList = new ArrayList<>(2);
            contextList.add(server.createContext(METRICS_CONTEXT_PREFIX + "/prometheus", metricsHandler));
            contextList.add(server.createContext(METRICS_CONTEXT_PREFIX + "/sys", metricsHandler));

            optionalAuthenticator.ifPresent(
                    authenticator -> contextList.forEach(
                            context -> context.setAuthenticator(authenticator)));

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
    public Optional<MeterRegistry> cloudWatchMeterRegistry(
            final DataPrepperConfiguration dataPrepperConfiguration,
            final CloudWatchMeterRegistryProvider cloudWatchMeterRegistryProvider
    ) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.CloudWatch)) {
            final CloudWatchMeterRegistry meterRegistry = cloudWatchMeterRegistryProvider.getCloudWatchMeterRegistry();
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
            final List<Optional<MeterRegistry>> optionalMeterRegistries
    ) {
        final CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();

        LOG.debug("{} Meter Binder beans registered.", meterBinders.size());
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
