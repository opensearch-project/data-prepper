/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.opensearch.dataprepper.core.meter.EMFLoggingMeterRegistry;
import org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.parser.model.MetricRegistryType;
import org.opensearch.dataprepper.core.pipeline.server.CloudWatchMeterRegistryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.core.exception.SdkClientException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

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

    @Bean
    public JvmMemoryAggregateMetrics jvmMemoryAggregateMetrics() {
        return new JvmMemoryAggregateMetrics();
    }

    private void configureMetricRegistry(final Map<String, String> metricTags,
                                         final List<MetricTagFilter> metricTagFilters,
                                         final MeterRegistry meterRegistry) {
        meterRegistry.config().meterFilter(new CustomTagsMeterFilter(metricTags, metricTagFilters));
    }

    @Bean
    public PrometheusMeterRegistry prometheusMeterRegistry(final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.Prometheus)) {
            final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            configureMetricRegistry(
                    dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(), meterRegistry
            );

            return meterRegistry;
        }
        else {
            return null;
        }
    }

    @Bean
    public CloudWatchMeterRegistryProvider cloudWatchMeterRegistryProvider(
            final DataPrepperConfiguration dataPrepperConfiguration
    ) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.CloudWatch)) {
            return new CloudWatchMeterRegistryProvider();
        }
        else {
            return null;
        }
    }

    @Bean
    public MeterRegistry cloudWatchMeterRegistry(
            final DataPrepperConfiguration dataPrepperConfiguration,
            @Autowired(required = false) @Nullable final CloudWatchMeterRegistryProvider cloudWatchMeterRegistryProvider
    ) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.CloudWatch)) {
            if (cloudWatchMeterRegistryProvider == null) {
                throw new IllegalStateException(
                        "configuration required configure cloudwatch meter registry but one could not be configured");
            }

            try {
                final CloudWatchMeterRegistry meterRegistry = cloudWatchMeterRegistryProvider.getCloudWatchMeterRegistry();
                configureMetricRegistry(
                        dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(), meterRegistry
                );

                return meterRegistry;
            } catch (final SdkClientException e) {
                LOG.warn("Unable to configure Cloud Watch Meter Registry but Meter Registry was requested in Data Prepper Configuration");
                throw new RuntimeException("Unable to initialize Cloud Watch Meter Registry", e);
            }
        }
        else {
            return null;
        }
    }

    @Bean
    public EMFLoggingMeterRegistry emfLoggingMeterRegistry(final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.EmbeddedMetricsFormat)) {
            final EMFLoggingMeterRegistry meterRegistry = new EMFLoggingMeterRegistry();
            configureMetricRegistry(
                    dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(), meterRegistry
            );
            return meterRegistry;
        } else {
            return null;
        }
    }

    @Bean
    public CompositeMeterRegistry systemMeterRegistry(
            final List<MeterBinder> meterBinders,
            final List<MeterRegistry> meterRegistries
    ) {
        final CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();

        LOG.debug("{} Meter Binder beans registered.", meterBinders.size());
        meterBinders.forEach(binder -> binder.bindTo(compositeMeterRegistry));

        meterRegistries.forEach(meterRegistry -> {
            compositeMeterRegistry.add(meterRegistry);
            Metrics.addRegistry(meterRegistry);
        });

        return compositeMeterRegistry;
    }
}
