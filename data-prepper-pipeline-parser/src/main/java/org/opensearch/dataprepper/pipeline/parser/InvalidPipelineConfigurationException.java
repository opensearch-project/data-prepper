package org.opensearch.dataprepper.pipeline.parser;

/**
 * Exception thrown when pipeline configuration validation fails.
 */
public class InvalidPipelineConfigurationException extends RuntimeException {
    public InvalidPipelineConfigurationException(String message) {
        super(message);
    }

    public InvalidPipelineConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}