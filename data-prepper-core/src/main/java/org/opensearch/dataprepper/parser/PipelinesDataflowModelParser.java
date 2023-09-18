package org.opensearch.dataprepper.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
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
        final List<PipelinesDataFlowModel> pipelinesDataFlowModels = parsePipelineConfigurationFiles();
        return mergePipelinesDataModels(pipelinesDataFlowModels);
    }

    private void validateDataPrepperVersion(final DataPrepperVersion version) {
        if (Objects.nonNull(version) && !DataPrepperVersion.getCurrentVersion().compatibleWith(version)) {
            LOG.error("The version: {} is not compatible with the current version: {}", version, DataPrepperVersion.getCurrentVersion());
            throw new ParseException(format("The version: %s is not compatible with the current version: %s",
                    version, DataPrepperVersion.getCurrentVersion()));
        }
    }

    private List<PipelinesDataFlowModel> parsePipelineConfigurationFiles() {
        final File configurationLocation = new File(pipelineConfigurationFileLocation);

        if (configurationLocation.isFile()) {
            return Stream.of(configurationLocation).map(this::parsePipelineConfigurationFile)
                    .filter(Objects::nonNull).collect(Collectors.toList());
        } else if (configurationLocation.isDirectory()) {
            FileFilter yamlFilter = pathname -> (pathname.getName().endsWith(".yaml") || pathname.getName().endsWith(".yml"));
            List<PipelinesDataFlowModel> pipelinesDataFlowModels = Stream.of(configurationLocation.listFiles(yamlFilter))
                    .map(this::parsePipelineConfigurationFile)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (pipelinesDataFlowModels.isEmpty()) {
                LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
                throw new ParseException(
                        format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
            }

            return pipelinesDataFlowModels;
        } else {
            LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
        }
    }

    private PipelinesDataFlowModel parsePipelineConfigurationFile(final File pipelineConfigurationFile) {
        try (final InputStream pipelineConfigurationInputStream = new FileInputStream(pipelineConfigurationFile)) {
            LOG.info("Reading pipeline configuration from {}", pipelineConfigurationFile.getName());
            final PipelinesDataFlowModel pipelinesDataFlowModel = OBJECT_MAPPER.readValue(pipelineConfigurationInputStream,
                    PipelinesDataFlowModel.class);

            final DataPrepperVersion version = pipelinesDataFlowModel.getDataPrepperVersion();
            validateDataPrepperVersion(version);

            return pipelinesDataFlowModel;
        } catch (IOException e) {
            if (e instanceof FileNotFoundException) {
                LOG.warn("Pipeline configuration file {} not found", pipelineConfigurationFile.getName());
                return null;
            }
            LOG.error("Failed to parse the configuration file {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Failed to parse the configuration file %s", pipelineConfigurationFileLocation), e);
        }
    }

    private PipelinesDataFlowModel mergePipelinesDataModels(
            final List<PipelinesDataFlowModel> pipelinesDataFlowModels) {
        final Map<String, PipelineModel> pipelinesDataFlowModelMap = pipelinesDataFlowModels.stream()
                .map(PipelinesDataFlowModel::getPipelines)
                .flatMap(pipelines -> pipelines.entrySet().stream())
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        final List<PipelineExtensions> pipelineExtensionsList = pipelinesDataFlowModels.stream()
                .map(PipelinesDataFlowModel::getPipelineExtensions)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (pipelineExtensionsList.size() > 1 ||
                (pipelineExtensionsList.size() == 1 && pipelinesDataFlowModels.size() > 1)) {
            throw new ParseException(
                    "pipeline_configurations and definition must all be defined in a single YAML file if pipeline_configurations is configured.");
        }
        return pipelineExtensionsList.isEmpty() ? new PipelinesDataFlowModel(pipelinesDataFlowModelMap) :
                new PipelinesDataFlowModel(pipelineExtensionsList.get(0), pipelinesDataFlowModelMap);
    }
}
