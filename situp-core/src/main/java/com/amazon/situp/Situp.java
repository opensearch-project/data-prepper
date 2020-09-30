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
 * the same instance reference can be further used to {@link #stop()} the execution.
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
        return initiateExecution();
    }

    /**
     * Terminates the execution of SITUP.
     * TODO return boolean status of the stop request
     */
    public void stop() {
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.stop();
            LOG.info("Successfully submitted request to stop execution of pipeline {}", name);
        });
    }

    private boolean initiateExecution() {
        LOG.info("Successfully parsed the configuration file, Triggering pipeline execution");
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.execute();
            LOG.info("Successfully triggered execution of pipeline {}", name);
        });
        return true;
    }
}