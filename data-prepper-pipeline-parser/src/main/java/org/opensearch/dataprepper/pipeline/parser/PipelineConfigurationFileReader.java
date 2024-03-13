package org.opensearch.dataprepper.pipeline.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class PipelineConfigurationFileReader implements PipelineConfigurationReader {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationFileReader.class);
    private final String pipelineConfigurationFileLocation;

    public PipelineConfigurationFileReader(final String pipelineConfigurationFileLocation) {
        this.pipelineConfigurationFileLocation = pipelineConfigurationFileLocation;
    }

    @Override
    public List<InputStream> getPipelineConfigurationInputStreams() {
        return getInputStreamsForConfigurationFiles();
    }

    private List<InputStream> getInputStreamsForConfigurationFiles() {
        final File configurationLocation = new File(pipelineConfigurationFileLocation);

        if (configurationLocation.isFile()) {
            return Stream.of(configurationLocation).map(this::getInputStreamForFile)
                    .filter(Objects::nonNull).collect(Collectors.toList());
        } else if (configurationLocation.isDirectory()) {
            FileFilter yamlFilter = pathname -> (pathname.getName().endsWith(".yaml") || pathname.getName().endsWith(".yml"));
            List<InputStream> inputStreams = Stream.of(configurationLocation.listFiles(yamlFilter))
                    .map(this::getInputStreamForFile)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (inputStreams.isEmpty()) {
                LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
                throw new ParseException(
                        format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
            }

            return inputStreams;
        } else {
            LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
        }
    }

    private InputStream getInputStreamForFile(final File pipelineConfigurationFile) {

        try {
            return new FileInputStream(pipelineConfigurationFile);
        } catch (IOException e) {
            LOG.warn("Pipeline configuration file {} not found", pipelineConfigurationFile.getName());
            return null;
        }
    }
}
