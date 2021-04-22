/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * DataPrepper is the entry point into the execution engine. An instance of this class is provided by
 * {@link #getInstance()} method and the same can eb used to trigger execution via {@link #execute(String)} ()} of the
 * {@link Pipeline} with default configuration or {@link #execute(String)} to provide custom configuration file. Also,
 * the same instance reference can be further used to {@link #shutdown()} the execution.
 */
public class DataPrepper {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepper.class);

    private static final PrometheusMeterRegistry sysJVMMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private Map<String, Pipeline> transformationPipelines;

    private static volatile DataPrepper dataPrepper;

    private static DataPrepperServer dataPrepperServer;
    private static DataPrepperConfiguration configuration = DataPrepperConfiguration.DEFAULT_CONFIG;

    static {
        new ClassLoaderMetrics().bindTo(sysJVMMeterRegistry);
        new JvmMemoryMetrics().bindTo(sysJVMMeterRegistry);
        new JvmGcMetrics().bindTo(sysJVMMeterRegistry);
        new ProcessorMetrics().bindTo(sysJVMMeterRegistry);
        new JvmThreadMetrics().bindTo(sysJVMMeterRegistry);
    }

    /**
     * Set the DataPrepperConfiguration from a file
     * @param configurationFile File containing DataPrepperConfiguration yaml
     */
    public static void configure(final String configurationFile) {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(configurationFile));

        configuration = dataPrepperConfiguration;
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
        startPrometheusBackend();
        dataPrepperServer = new DataPrepperServer(this);
    }

    public static PrometheusMeterRegistry getSysJVMMeterRegistry() {
        return sysJVMMeterRegistry;
    }

    /**
     * Create a PrometheusMeterRegistry for this DataPrepper and register it with the global registry
     */
    private static void startPrometheusBackend() {
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);
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
        if (transformationPipelines.size() == 0){
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
    public void shutdownDataPrepperServer(){
        dataPrepperServer.stop();
    }

    /**
     * Triggers shutdown of the provided pipeline, no-op if the pipeline does not exist.
     * @param pipeline name of the pipeline
     */
    public void shutdown(final String pipeline) {
        if(transformationPipelines.containsKey(pipeline)) {
            transformationPipelines.get(pipeline).shutdown();
        }
    }

    public Map<String, Pipeline> getTransformationPipelines() {
        return  transformationPipelines;
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