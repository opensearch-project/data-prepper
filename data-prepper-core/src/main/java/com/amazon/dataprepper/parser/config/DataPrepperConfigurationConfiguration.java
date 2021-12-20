package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.SimpleCommandLinePropertySource;

import java.io.File;

@Configuration
public class DataPrepperConfigurationConfiguration {
    private static final String POSITIONAL_COMMAND_LINE_ARGUMENTS = "nonOptionArgs";
    private static final String COMMAND_LINE_ARG_DELIMITER = ",";
    private static final Integer DATA_PREPPER_CONFIG_POSITON = 1;

    @Bean
    public CompositeMeterRegistry systemMeterRegistry() {
        return new CompositeMeterRegistry();
    }

    @Bean
    public DataPrepperConfiguration dataPrepperDefaultConfiguration(
            final Environment environment,
            final CompositeMeterRegistry systemMeterRegistry)
    {
        final String commandLineArgs = environment.getProperty(POSITIONAL_COMMAND_LINE_ARGUMENTS);
        DataPrepperConfiguration configuration;
        if (commandLineArgs != null) {
            String[] args = commandLineArgs.split(COMMAND_LINE_ARG_DELIMITER);
            if (args.length > DATA_PREPPER_CONFIG_POSITON) {
                final String configurationFilePath = args[DATA_PREPPER_CONFIG_POSITON];
                final File configurationFile = new File(configurationFilePath);
                configuration = DataPrepperConfiguration.fromFile(configurationFile);
            }
        }

        if (configuration == null) {
            configuration = new DataPrepperConfiguration();
        }

        configuration.getMetricRegistryTypes().forEach(metricRegistryType ->
                systemMeterRegistry.add(MetricRegistryType.getDefaultMeterRegistryForType(metricRegistryType)));

        new ClassLoaderMetrics().bindTo(systemMeterRegistry);
        new JvmMemoryMetrics().bindTo(systemMeterRegistry);
        new JvmGcMetrics().bindTo(systemMeterRegistry);
        new ProcessorMetrics().bindTo(systemMeterRegistry);
        new JvmThreadMetrics().bindTo(systemMeterRegistry);

        return configuration;
    }
}
