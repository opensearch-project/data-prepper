package com.amazon.dataprepper;

import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * DataPrepper is the entry point into the execution engine. An instance of this class is provided by
 * {@link #getInstance()} method and the same can eb used to trigger execution via {@link #execute(String)} ()} of the
 * {@link Pipeline} with default configuration or {@link #execute(String)} to provide custom configuration file. Also,
 * the same instance reference can be further used to {@link #shutdown()} the execution.
 */
public class DataPrepper {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepper.class);

    private Map<String, Pipeline> transformationPipelines;

    private static volatile DataPrepper dataPrepper;

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
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.shutdown();
        });
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

    private boolean initiateExecution() {
        transformationPipelines.forEach((name, pipeline) -> {
            pipeline.execute();
        });
        return true;
    }
}