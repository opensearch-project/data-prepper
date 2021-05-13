package com.amazon.dataprepper;

import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * DataPrepper is the entry point into the execution engine. An instance of this class is provided by
 * {@link #getInstance()} method and the same can eb used to trigger execution via {@link #execute(String)} ()} of the
 * {@link Pipeline} with default configuration or {@link #execute(String)} to provide custom configuration file. Also,
 * the same instance reference can be further used to {@link #shutdown()} the execution.
 */
public class DataPrepper {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepper.class);

    private static final CompositeMeterRegistry systemMeterRegistry = new CompositeMeterRegistry();

    private Map<String, Pipeline> transformationPipelines;

    private static volatile DataPrepper dataPrepper;

    private static DataPrepperServer dataPrepperServer;
    private static DataPrepperConfiguration configuration;

    /**
     * Set the DataPrepperConfiguration from file
     *
     * @param configurationFile File containing DataPrepperConfiguration yaml
     */
    public static void configure(final String configurationFile) {
        configuration = DataPrepperConfiguration.fromFile(new File(configurationFile));
        configureMeterRegistry();
    }

    /**
     * Set the DataPrepperConfiguration with defaults
     */
    public static void configureWithDefaults() {
        configuration = DataPrepperConfiguration.DEFAULT_CONFIG;
        configureMeterRegistry();
    }

    public static DataPrepper getInstance() {
        if (dataPrepper == null) {
            synchronized (DataPrepper.class) {
                if (dataPrepper == null)
                    dataPrepper = new DataPrepper();
            }
        }
        return dataPrepper;
    }

    private DataPrepper() {
        if (dataPrepper != null) {
            throw new RuntimeException("Please use getInstance() for an instance of this Data Prepper");
        }
        startMeterRegistryForDataPrepper();
        dataPrepperServer = new DataPrepperServer(this);
    }

    /**
     * Creates instances of configured MeterRegistry and registers to {@link Metrics} globalRegistry to be used by
     * Meters.
     */
    private static void startMeterRegistryForDataPrepper() {
        final List<MetricRegistryType> configuredMetricRegistryTypes = configuration.getMetricRegistryTypes();
        configuredMetricRegistryTypes.forEach(metricRegistryType -> Metrics.addRegistry(MetricRegistryType
                .getDefaultMeterRegistryForType(metricRegistryType)));
    }

    private static void configureMeterRegistry() {
        configuration.getMetricRegistryTypes().forEach(metricRegistryType ->
                systemMeterRegistry.add(MetricRegistryType.getDefaultMeterRegistryForType(metricRegistryType)));
        new ClassLoaderMetrics().bindTo(systemMeterRegistry);
        new JvmMemoryMetrics().bindTo(systemMeterRegistry);
        new JvmGcMetrics().bindTo(systemMeterRegistry);
        new ProcessorMetrics().bindTo(systemMeterRegistry);
        new JvmThreadMetrics().bindTo(systemMeterRegistry);
    }

    public static CompositeMeterRegistry getSystemMeterRegistry() {
        return systemMeterRegistry;
    }

    /**
     * Executes Data Prepper engine using the default configuration file/
     *
     * @param configurationFileLocation the location of the configuration file
     * @return true if the execute successfully initiates the Data Prepper
     */
    public boolean execute(final String configurationFileLocation) {
        LOG.info("Using {} configuration file", configurationFileLocation);
        final PipelineParser pipelineParser = new PipelineParser(configurationFileLocation);
        transformationPipelines = pipelineParser.parseConfiguration();
        if (transformationPipelines.size() == 0) {
            LOG.error("No valid pipeline is available for execution, exiting");
            System.exit(1);
        }
        return initiateExecution();
    }

    /**
     * Triggers the shutdown of all configured valid pipelines.
     */
    public void shutdown() {
        for (final Pipeline pipeline : transformationPipelines.values()) {
            LOG.info("Shutting down pipeline: {}", pipeline.getName());
            pipeline.shutdown();
        }
    }

    /**
     * Triggers shutdown of the Data Prepper server.
     */
    public void shutdownDataPrepperServer() {
        dataPrepperServer.stop();
    }

    /**
     * Triggers shutdown of the provided pipeline, no-op if the pipeline does not exist.
     *
     * @param pipeline name of the pipeline
     */
    public void shutdown(final String pipeline) {
        if (transformationPipelines.containsKey(pipeline)) {
            transformationPipelines.get(pipeline).shutdown();
        }
    }

    public Map<String, Pipeline> getTransformationPipelines() {
        return transformationPipelines;
    }

    public static DataPrepperConfiguration getConfiguration() {
        return configuration;
    }

    private boolean initiateExecution() {
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.execute();
        });
        dataPrepperServer.start();
        return true;
    }
}