/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineParser;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.PipelineObserver;
import org.opensearch.dataprepper.pipeline.PipelinesProvider;
import org.opensearch.dataprepper.pipeline.server.DataPrepperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * DataPrepper is the entry point into the execution engine. The instance can be used to trigger execution via
 * {@link #execute()} of the {@link Pipeline} with default configuration or {@link #execute()} to
 * provide custom configuration file. Also, the same instance reference can be further used to {@link #shutdownPipelines()} the
 * execution.
 */
@Named
public class DataPrepper implements PipelinesProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepper.class);
    private static final String DATAPREPPER_SERVICE_NAME = "DATAPREPPER_SERVICE_NAME";
    private static final String DEFAULT_SERVICE_NAME = "dataprepper";
    private static final int MAX_RETRIES = 100;

    private final PluginFactory pluginFactory;
    private final PeerForwarderServer peerForwarderServer;
    private final PipelinesObserver pipelinesObserver;
    private final Map<String, Pipeline> transformationPipelines;
    private final Predicate<Map<String, Pipeline>> shouldShutdownOnPipelineFailurePredicate;

    // TODO: Remove DataPrepperServer dependency on DataPrepper
    @Inject
    @Lazy
    private DataPrepperServer dataPrepperServer;
    private DataPrepperShutdownListener shutdownListener;

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
            final PeerForwarderServer peerForwarderServer,
            final Predicate<Map<String, Pipeline>> shouldShutdownOnPipelineFailurePredicate) {
        this.pluginFactory = pluginFactory;

        transformationPipelines = pipelineParser.parseConfiguration();
        this.shouldShutdownOnPipelineFailurePredicate = shouldShutdownOnPipelineFailurePredicate;
        if (transformationPipelines.size() == 0) {
            throw new RuntimeException("No valid pipeline is available for execution, exiting");
        }
        this.peerForwarderServer = peerForwarderServer;
        pipelinesObserver = new PipelinesObserver();
    }

    /**
     * Executes Data Prepper engine using the default configuration file
     *
     * @return true if execute successfully initiates the Data Prepper
     */
    public boolean execute() {
        peerForwarderServer.start();
        Set<String> waitingPipelineNames = transformationPipelines.keySet();
        int numRetries = 0;
        while (waitingPipelineNames.size() > 0 && numRetries++ < MAX_RETRIES) {
            Set<String> uninitializedPipelines = new HashSet<String>();
            Iterator pipelineIter = waitingPipelineNames.iterator();
            while (pipelineIter.hasNext()) {
                String pipelineName = (String)pipelineIter.next();
                if (!transformationPipelines.get(pipelineName).isReady()) {
                    uninitializedPipelines.add(pipelineName);
                }
            }
            waitingPipelineNames = uninitializedPipelines;
            if (waitingPipelineNames.size() > 0) {
                try {
                    Thread.sleep(5000);
                } catch (Exception e){}
            }
        }
        if (waitingPipelineNames.size() > 0) {
            LOG.info("One or more Pipelines are not ready even after {} retries. Shutting down pipelines", numRetries);
            shutdownPipelines();
            throw new RuntimeException("Failed to start pipelines");
        }
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.addShutdownObserver(pipelinesObserver);
        });
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.execute();
        });
        dataPrepperServer.start();
        return true;
    }

    public void shutdown() {
        shutdownPipelines();
        shutdownServers();
    }

    /**
     * Triggers the shutdown of all configured valid pipelines.
     */
    public void shutdownPipelines() {
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.removeShutdownObserver(pipelinesObserver);
        });

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
        if(shutdownListener != null) {
            shutdownListener.handleShutdown();
        }
    }

    /**
     * Triggers shutdown of the provided pipeline, no-op if the pipeline does not exist.
     *
     * @param pipeline name of the pipeline
     */
    public void shutdownPipelines(final String pipeline) {
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

    public void registerShutdownHandler(final DataPrepperShutdownListener shutdownListener) {
        this.shutdownListener = shutdownListener;
    }

    private class PipelinesObserver implements PipelineObserver {
        @Override
        public void shutdown(final Pipeline pipeline) {
            pipeline.removeShutdownObserver(pipelinesObserver);
            transformationPipelines.remove(pipeline.getName());

            LOG.info("Pipeline has shutdown. '{}'. There are {} pipelines remaining.", pipeline.getName(), transformationPipelines.size());
            if(shouldShutdownOnPipelineFailurePredicate.test(transformationPipelines)) {
                DataPrepper.this.shutdown();
            }
        }
    }
}
