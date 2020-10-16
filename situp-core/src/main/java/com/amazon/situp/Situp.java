package com.amazon.situp;

import com.amazon.situp.parser.PipelineParser;
import com.amazon.situp.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * SITUP is the entry point into the execution engine. An instance of this class is provided by
 * {@link #getInstance()} method and the same can eb used to trigger execution via {@link #execute()} of the
 * {@link Pipeline} with default configuration or {@link #execute(String)} to provide custom configuration file. Also,
 * the same instance reference can be further used to {@link #shutdown()} the execution.
 */
public class Situp {
    private static final Logger LOG = LoggerFactory.getLogger(Situp.class);

    private static final String DEFAULT_CONFIG_LOCATION = "situp-core/src/main/resources/situp-default.yml";
    private Map<String, Pipeline> transformationPipelines;

    private static volatile Situp situp;

    public static Situp getInstance() {
        if (situp == null) {
            synchronized (Situp.class) {
                if (situp == null)
                    situp = new Situp();
            }
        }
        return situp;
    }

    private Situp() {
        if (situp != null) {
            throw new RuntimeException("Please use getInstance() for an instance of this SITUP");
        }
    }

    /**
     * Executes SITUP engine using the default configuration file/
     *
     * @return true if the execute successfully initiates the SITUP
     */
    public boolean execute() {
        return execute(DEFAULT_CONFIG_LOCATION);
    }

    /**
     * Executes SITUP engine using the default configuration file/
     *
     * @param configurationFileLocation the location of the configuration file
     * @return true if the execute successfully initiates the SITUP
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
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.shutdown();
        });
    }

    /**
     * Terminates the provided pipeline
     */
    public void shutdown(final String pipeline) {
        if(transformationPipelines.containsKey(pipeline)) {
            transformationPipelines.get(pipeline).shutdown();
        }
    }

    private boolean initiateExecution() {
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.execute();
        });
        return true;
    }
}