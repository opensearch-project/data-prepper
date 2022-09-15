/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineParser;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.server.DataPrepperServer;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import javax.inject.Inject;
import javax.inject.Named;
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

    private final PluginFactory pluginFactory;
    private final PeerForwarderServer peerForwarderServer;
    private Map<String, Pipeline> transformationPipelines;

    // TODO: Remove DataPrepperServer dependency on DataPrepper
    @Inject
    @Lazy
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
            final PipelineParser pipelineParser,
            final PluginFactory pluginFactory,
            final PeerForwarderServer peerForwarderServer
            ) {
        this.pluginFactory = pluginFactory;

        transformationPipelines = pipelineParser.parseConfiguration();
        if (transformationPipelines.size() == 0) {
            throw new RuntimeException("No valid pipeline is available for execution, exiting");
        }
        this.peerForwarderServer = peerForwarderServer;
    }

    /**
     * Executes Data Prepper engine using the default configuration file
     *
     * @return true if execute successfully initiates the Data Prepper
     */
    public boolean execute() {
        peerForwarderServer.start();
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
     * Triggers shutdown of the Data Prepper and Peer Forwarder server.
     */
    public void shutdownServers() {
        dataPrepperServer.stop();
        peerForwarderServer.stop();
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