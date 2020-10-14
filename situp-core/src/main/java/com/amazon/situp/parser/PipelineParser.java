package com.amazon.situp.parser;

import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.model.source.Source;
import com.amazon.situp.parser.model.PipelineConfiguration;
import com.amazon.situp.pipeline.Pipeline;
import com.amazon.situp.pipeline.PipelineConnector;
import com.amazon.situp.plugins.buffer.BlockingBuffer;
import com.amazon.situp.plugins.buffer.BufferFactory;
import com.amazon.situp.plugins.processor.ProcessorFactory;
import com.amazon.situp.plugins.sink.SinkFactory;
import com.amazon.situp.plugins.source.SourceFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

@SuppressWarnings("rawtypes")
public class PipelineParser {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    private static final String PIPELINE_TYPE = "pipeline";
    private static final String ATTRIBUTE_NAME = "name";
    private static final int DEFAULT_READ_BATCH_DELAY = 3_000;
    private final String configurationFileLocation;
    private final Map<String, PipelineConnector> sourceConnectorMap = new HashMap<>();

    public PipelineParser(final String configurationFileLocation) {
        this.configurationFileLocation = configurationFileLocation;
    }

    /**
     * Parses the configuration file into Pipeline
     */
    public Map<String, Pipeline> parseConfiguration() {
        try {
            final Map<String, PipelineConfiguration> pipelineConfigurationMap = OBJECT_MAPPER.readValue(
                    new File(configurationFileLocation),
                    new TypeReference<Map<String, PipelineConfiguration>>() {
                    });
            final List<String> allPipelineNames = PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigurationMap);
            final Map<String, Pipeline> pipelineMap = new HashMap<>();
            for (String pipelineName : allPipelineNames) {
                if (!pipelineMap.containsKey(pipelineName)) {
                    buildPipelineFromConfiguration(pipelineName, pipelineConfigurationMap, pipelineMap);
                }
            }
            return pipelineMap;
        } catch (IOException e) {
            throw new ParseException(format("Failed to parse the configuration file %s", configurationFileLocation), e);
        }
    }

    private void buildPipelineFromConfiguration(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipelineName);
        LOG.info("Building pipeline [{}] from provided configuration", pipelineName);
        try {
            final PluginSetting sourceSetting = pipelineConfiguration.getSourcePluginSetting();
            final Optional<Source> pipelineSource = getSourceIfPipelineType(pipelineName, sourceSetting,
                    pipelineMap, pipelineConfigurationMap);
            final Source source = pipelineSource.orElseGet(() -> SourceFactory.newSource(sourceSetting));

            LOG.info("Building buffer for the pipeline [{}]", pipelineName);
            final Buffer buffer = getBufferOrDefault(pipelineConfiguration.getBufferPluginSetting());

            LOG.info("Building processors for the pipeline [{}]", pipelineName);
            final List<Processor> processors = pipelineConfiguration.getProcessorPluginSettings().stream()
                    .map(ProcessorFactory::newProcessor)
                    .collect(Collectors.toList());
            final int processorThreads = getWorkersOrDefault(pipelineConfiguration.getWorkers());
            final int readBatchDelay = getDelayOrDefault(pipelineConfiguration.getReadBatchDelay());

            LOG.info("Building sinks for the pipeline [{}]", pipelineName);
            final List<Sink> sinks = pipelineConfiguration.getSinkPluginSettings().stream()
                    .map(this::buildSinkOrConnector).collect(Collectors.toList());

            final Pipeline pipeline = new Pipeline(pipelineName, source, buffer, processors, sinks, processorThreads, readBatchDelay);
            pipelineMap.put(pipelineName, pipeline);
        } catch (Exception ex) {
            //If pipeline construction errors out, we will skip that pipeline and proceed
            LOG.error("Construction of pipeline components failed, skipping building of pipeline [{}]", pipelineName, ex);
        }

    }

    private Buffer getBufferOrDefault(final PluginSetting bufferPluginSetting) {
        return bufferPluginSetting == null ? new BlockingBuffer() : BufferFactory.newBuffer(bufferPluginSetting);
    }

    private int getWorkersOrDefault(final Integer workers) {
        return workers == null ? getDefaultProcessorThreads() : workers;
    }

    private int getDelayOrDefault(final Integer readBatchDelay) {
        return readBatchDelay == null ? DEFAULT_READ_BATCH_DELAY : readBatchDelay;
    }

    /**
     * TODO Implement this to use CPU cores of the executing machine
     */
    private int getDefaultProcessorThreads() {
        return 1; //Runtime.getRuntime().availableProcessors()
    }

    private Optional<Source> getSourceIfPipelineType(
            final String sourcePipelineName,
            final PluginSetting pluginSetting,
            final Map<String, Pipeline> pipelineMap,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        LOG.info("Building [{}] as source component for the pipeline [{}]", pluginSetting.getName(), sourcePipelineName);
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            if (!sourceConnectorMap.containsKey(sourcePipelineName)) {
                LOG.info("Source of pipeline [{}] requires building of pipeline [{}]", sourcePipelineName,
                        pipelineNameOptional.get());
                //Build connected pipeline for the pipeline connector to be available
                buildPipelineFromConfiguration(pipelineNameOptional.get(), pipelineConfigurationMap, pipelineMap);
            }
            return Optional.of(sourceConnectorMap.get(sourcePipelineName));
        }
        return Optional.empty();
    }

    private Sink buildSinkOrConnector(final PluginSetting pluginSetting) {
        LOG.info("Building [{}] as sink component", pluginSetting.getName());
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final PipelineConnector pipelineConnector = new PipelineConnector();
            sourceConnectorMap.put(pipelineNameOptional.get(), pipelineConnector); //TODO retrieve from parent Pipeline using name
            return pipelineConnector;
        } else {
            return SinkFactory.newSink(pluginSetting);
        }
    }

    private Optional<String> getPipelineNameIfPipelineType(final PluginSetting pluginSetting) {
        if (PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME) != null) {
            //Validator marked valid config with type as pipeline will have attribute name
            return Optional.of((String) pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME));
        }
        return Optional.empty();
    }
}