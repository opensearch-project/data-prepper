/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Map;

/**
 * DataPrepper is the entry point into the execution engine. The instance can be used to trigger execution via
 * {@link #execute()} of the {@link Pipeline} with default configuration or {@link #execute()} to
 * provide custom configuration file. Also, the same instance reference can be further used to {@link #shutdown()} the
 * execution.
 */
@Named
public class DataPrepper {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepper.class);
    private static final String DATAPREPPER_SERVICE_NAME = "DATAPREPPER_SERVICE_NAME";
    private static final String DEFAULT_SERVICE_NAME = "dataprepper";

    private static final CompositeMeterRegistry systemMeterRegistry = new CompositeMeterRegistry();

    private final DataPrepperConfiguration configuration;
    private final PluginFactory pluginFactory;
    private Map<String, Pipeline> transformationPipelines;

    // TODO: Remove DataPrepperServer dependency on DataPrepper
    @Inject
    private DataPrepperServer dataPrepperServer;

    /**
     * returns serviceName if exists or default serviceName
     * @return serviceName for data-prepper
     */
    public static String getServiceNameForMetrics() {
        final String serviceName = System.getenv(DATAPREPPER_SERVICE_NAME);
        return StringUtils.isNotBlank(serviceName) ? serviceName : DEFAULT_SERVICE_NAME;
    }

    @Inject
    public DataPrepper(
            final DataPrepperConfiguration configuration,
            final PipelineParser pipelineParser,
            final PluginFactory pluginFactory
    ) {
        this.configuration = configuration;
        this.pluginFactory = pluginFactory;

        transformationPipelines = pipelineParser.parseConfiguration();
        if (transformationPipelines.size() == 0) {
            throw new RuntimeException("No valid pipeline is available for execution, exiting");
        }
    }

    public static CompositeMeterRegistry getSystemMeterRegistry() {
        return systemMeterRegistry;
    }

    /**
     * Executes Data Prepper engine using the default configuration file
     *
     * @return true if execute successfully initiates the Data Prepper
     */
    public boolean execute() {
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.execute();
        });
        dataPrepperServer.start();
        return true;
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
}