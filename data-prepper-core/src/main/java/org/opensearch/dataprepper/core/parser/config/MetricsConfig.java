/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.registry.otlp.AggregationTemporality;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import org.opensearch.dataprepper.core.DataPrepper;
import org.opensearch.dataprepper.core.meter.EMFLoggingMeterRegistry;
import org.opensearch.dataprepper.core.meter.EMFLoggingRegistryConfig;
import org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.parser.model.MetricRegistryType;
import org.opensearch.dataprepper.core.parser.model.OtlpMetricsConfig;
import org.opensearch.dataprepper.core.pipeline.server.CloudWatchMeterRegistryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.core.exception.SdkClientException;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics.JVM_MEMORY_USED;

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
                                         final List<String> disabledMetrics,
                                         final MeterRegistry meterRegistry){
        meterRegistry.config().meterFilter(new DisableMetricsFilter(disabledMetrics));
        meterRegistry.config().meterFilter(new CustomTagsMeterFilter(metricTags, metricTagFilters));
    }


    @Bean
    public PrometheusMeterRegistry prometheusMeterRegistry(final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.Prometheus)) {
            final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            // Earlier Prometheus clients dropped jvm.memory.used without an id tag; keep it out so existing sums do not double.
            meterRegistry.config().meterFilter(MeterFilter.deny(id -> JVM_MEMORY_USED.equals(id.getName()) && id.getTag("id") == null));
            configureMetricRegistry(
                    dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(),
                    dataPrepperConfiguration.getDisabledMetrics(),  meterRegistry
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
                        dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(),
                        dataPrepperConfiguration.getDisabledMetrics(), meterRegistry
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
            final EMFLoggingRegistryConfig config = createEMFLoggingRegistryConfig(dataPrepperConfiguration);
            final EMFLoggingMeterRegistry meterRegistry = new EMFLoggingMeterRegistry(config);
            configureMetricRegistry(
                    dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(),
                    dataPrepperConfiguration.getDisabledMetrics(), meterRegistry
            );
            return meterRegistry;
        } else {
            return null;
        }
    }

    @Bean
    public OtlpMeterRegistry otlpMeterRegistry(final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration.getMetricRegistryTypes().contains(MetricRegistryType.OTLP)) {
            final OtlpMeterRegistry meterRegistry = new OtlpMeterRegistry(
                    createOtlpConfig(dataPrepperConfiguration.getOtlpMetricsConfig()), Clock.SYSTEM);
            configureMetricRegistry(
                    dataPrepperConfiguration.getMetricTags(), dataPrepperConfiguration.getMetricTagFilters(),
                    dataPrepperConfiguration.getDisabledMetrics(), meterRegistry
            );
            return meterRegistry;
        } else {
            return null;
        }
    }

    @Bean
    public CompositeMeterRegistry systemMeterRegistry(
            final List<MeterBinder> meterBinders,
            final List<MeterRegistry> meterRegistries,
            final DataPrepperConfiguration dataPrepperConfiguration
    ) {
        final CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();

        meterRegistries.forEach(meterRegistry -> {
            configureMetricRegistry(
                    dataPrepperConfiguration.getMetricTags(),
                    dataPrepperConfiguration.getMetricTagFilters(),
                    dataPrepperConfiguration.getDisabledMetrics(),
                    meterRegistry
            );
            Metrics.addRegistry(meterRegistry);
            compositeMeterRegistry.add(meterRegistry);
        });

        meterBinders.forEach(binder -> binder.bindTo(compositeMeterRegistry));

        return compositeMeterRegistry;
    }

    private OtlpConfig createOtlpConfig(final OtlpMetricsConfig otlpMetricsConfig) {
        return new OtlpConfig() {
            @Override
            public String get(final String key) {
                return null;
            }

            @Override
            public String url() {
                return otlpMetricsConfig.getEndpoint();
            }

            @Override
            public AggregationTemporality aggregationTemporality() {
                return otlpMetricsConfig.getAggregationTemporality();
            }

            @Override
            public Duration step() {
                return otlpMetricsConfig.getStep();
            }

            @Override
            public Map<String, String> resourceAttributes() {
                final Map<String, String> resourceAttributes = new HashMap<>(otlpMetricsConfig.getResourceAttributes());
                resourceAttributes.putIfAbsent("service.name", DataPrepper.getServiceNameForMetrics());
                return resourceAttributes;
            }
        };
    }

    private EMFLoggingRegistryConfig createEMFLoggingRegistryConfig(final DataPrepperConfiguration dataPrepperConfiguration) {
        return new EMFLoggingRegistryConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public Map<String, String> additionalProperties() {
                return Collections.unmodifiableMap(dataPrepperConfiguration.getEmfAdditionalProperties());
            }
        };
    }

}
