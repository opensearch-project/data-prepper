package org.opensearch.dataprepper.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class PipelinesDataflowModelParser {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinesDataflowModelParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

    private final String pipelineConfigurationFileLocation;

    public PipelinesDataflowModelParser(final String pipelineConfigurationFileLocation) {
        this.pipelineConfigurationFileLocation = pipelineConfigurationFileLocation;
    }

    public PipelinesDataFlowModel parseConfiguration() {
        try (final InputStream mergedPipelineConfigurationFiles = mergePipelineConfigurationFiles()) {
            final PipelinesDataFlowModel pipelinesDataFlowModel = OBJECT_MAPPER.readValue(mergedPipelineConfigurationFiles,
                    PipelinesDataFlowModel.class);

            final DataPrepperVersion version = pipelinesDataFlowModel.getDataPrepperVersion();
            validateDataPrepperVersion(version);

            return pipelinesDataFlowModel;
        } catch (IOException e) {
            LOG.error("Failed to parse the configuration file {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Failed to parse the configuration file %s", pipelineConfigurationFileLocation), e);
        }
    }

    private void validateDataPrepperVersion(final DataPrepperVersion version) {
        if (Objects.nonNull(version) && !DataPrepperVersion.getCurrentVersion().compatibleWith(version)) {
            LOG.error("The version: {} is not compatible with the current version: {}", version, DataPrepperVersion.getCurrentVersion());
            throw new ParseException(format("The version: %s is not compatible with the current version: %s",
                    version, DataPrepperVersion.getCurrentVersion()));
        }
    }

    private InputStream mergePipelineConfigurationFiles() throws IOException {
        final File configurationLocation = new File(pipelineConfigurationFileLocation);

        if (configurationLocation.isFile()) {
            return new FileInputStream(configurationLocation);
        } else if (configurationLocation.isDirectory()) {
            FileFilter yamlFilter = pathname -> (pathname.getName().endsWith(".yaml") || pathname.getName().endsWith(".yml"));
            List<InputStream> configurationFiles = Stream.of(configurationLocation.listFiles(yamlFilter))
                    .map(file -> {
                        InputStream inputStream;
                        try {
                            inputStream = new FileInputStream(file);
                            LOG.info("Reading pipeline configuration from {}", file.getName());
                        } catch (FileNotFoundException e) {
                            inputStream = null;
                            LOG.warn("Pipeline configuration file {} not found", file.getName());
                        }
                        return inputStream;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (configurationFiles.isEmpty()) {
                LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
                throw new ParseException(
                        format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
            }

            return new SequenceInputStream(Collections.enumeration(configurationFiles));
        } else {
            LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
        }
    }
}
