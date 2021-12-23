package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

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

    @Bean
    public CompositeMeterRegistry systemMeterRegistry(
            final List<MeterBinder> meterBinders,
            final DataPrepperConfiguration dataPrepperConfiguration
    ) {
        final CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();

        meterBinders.forEach(binder -> binder.bindTo(meterRegistry));

        dataPrepperConfiguration.getMetricRegistryTypes().forEach(metricRegistryType -> {
            MeterRegistry registryForType = MetricRegistryType.getDefaultMeterRegistryForType(metricRegistryType);
            meterRegistry.add(registryForType);
            Metrics.addRegistry(registryForType);
        });

        return meterRegistry;
    }
}
