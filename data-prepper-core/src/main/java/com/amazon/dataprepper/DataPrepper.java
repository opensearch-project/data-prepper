/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.parser.model.MetricRegistryType;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import com.amazon.dataprepper.plugin.DefaultPluginFactory;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * DataPrepper is the entry point into the execution engine. An instance of this class is provided by
 * {@link #getInstance()} method and the same can eb used to trigger execution via {@link #execute(String)} ()} of the
 * {@link Pipeline} with default configuration or {@link #execute(String)} to provide custom configuration file. Also,
 * the same instance reference can be further used to {@link #shutdown()} the execution.
 */
@Singleton
public class DataPrepper {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepper.class);
    private static final String DATAPREPPER_SERVICE_NAME = "DATAPREPPER_SERVICE_NAME";
    private static final String DEFAULT_SERVICE_NAME = "dataprepper";

    private static final CompositeMeterRegistry systemMeterRegistry = new CompositeMeterRegistry();

    private final PluginFactory pluginFactory = new DefaultPluginFactory();
    private Map<String, Pipeline> transformationPipelines;

    private static volatile DataPrepper dataPrepper;

    private static DataPrepperServer dataPrepperServer;
    private final DataPrepperConfiguration configuration;

    /**
     * returns serviceName if exists or default serviceName
     * @return serviceName for data-prepper
     */
    public static String getServiceNameForMetrics() {
        final String serviceName = System.getenv(DATAPREPPER_SERVICE_NAME);
        return StringUtils.isNotBlank(serviceName) ? serviceName : DEFAULT_SERVICE_NAME;
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

    @Inject
    public DataPrepper(final DataPrepperConfiguration configuration) {
        this.configuration = configuration;
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
        final PipelineParser pipelineParser = new PipelineParser(configurationFileLocation, pluginFactory);
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
    public PluginFactory getPluginFactory() {
        return pluginFactory;
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