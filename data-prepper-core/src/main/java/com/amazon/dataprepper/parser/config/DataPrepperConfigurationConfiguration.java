package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.amazon.dataprepper.plugin.DefaultPluginFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.opensearch.dataprepper.logstash.LogstashConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Configuration
@ComponentScan(basePackageClasses = com.amazon.dataprepper.DataPrepper.class)
public class DataPrepperConfigurationConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperConfigurationConfiguration.class);
    private static final String POSITIONAL_COMMAND_LINE_ARGUMENTS = "nonOptionArgs";
    private static final String COMMAND_LINE_ARG_DELIMITER = ",";

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

    @Bean
    public DataPrepperArgs dataPrepperArgs(final Environment environment) {
        final String commandLineArgs = environment.getProperty(POSITIONAL_COMMAND_LINE_ARGUMENTS);

        LOG.info("Command line args: {}", commandLineArgs);

        if (commandLineArgs != null) {
            String[] args = commandLineArgs.split(COMMAND_LINE_ARG_DELIMITER);
            return new DataPrepperArgs(args);
        }
        else {
            throw new RuntimeException("Configuration file command line argument required but none found");
        }
    }

    @Bean
    public DataPrepperConfiguration dataPrepperConfiguration(final DataPrepperArgs dataPrepperArgs) {
        final String dataPrepperConfigFileLocation = dataPrepperArgs.getDataPrepperConfigFileLocation();
        if (dataPrepperConfigFileLocation != null) {
            final File configurationFile = new File(dataPrepperConfigFileLocation);
            return DataPrepperConfiguration.fromFile(configurationFile);
        }
        else {
            return new DataPrepperConfiguration();
        }
    }

    private static String checkForLogstashConfigurationAndConvert(String configurationFileLocation) {
        if (configurationFileLocation.endsWith(".conf")) {
            final LogstashConfigConverter logstashConfigConverter = new LogstashConfigConverter();
            final Path configurationDirectory = Paths.get(configurationFileLocation).toAbsolutePath().getParent();

            try {
                configurationFileLocation = logstashConfigConverter.convertLogstashConfigurationToPipeline(
                        configurationFileLocation, String.valueOf(configurationDirectory));
            } catch (IOException e) {
                LOG.error("Unable to read the Logstash configuration file", e);
            }
        }
        return configurationFileLocation;
    }
}
