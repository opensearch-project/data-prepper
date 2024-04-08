package org.opensearch.dataprepper.pipeline.parser;

import java.io.InputStream;
import java.util.List;

public interface PipelineConfigurationReader {

    /**
     *
     * @return a List of InputStream that contains each of the pipeline configurations.
     *         the caller of this method is responsible for closing these input streams after they are used
     */
    List<InputStream> getPipelineConfigurationInputStreams();

}
