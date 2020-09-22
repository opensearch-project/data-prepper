package com.amazon.situp.parser;

import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.Configuration;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    private static final String PIPELINE_TYPE = "pipeline";
    private static final String ATTRIBUTE_NAME = "name";
    private static final String ATTRIBUTE_WORKERS = "workers";
    private static final String ATTRIBUTE_DELAY = "delay";
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
                    new File(configurationFileLocation), new TypeReference<Map<String, PipelineConfiguration>>() {
                    });
            final List<String> allPipelineNames = PipelineConfigurationValidator.
                    validateAndGetPipelineNames(pipelineConfigurationMap);
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

        final PluginSetting sourceSetting = pipelineConfiguration.getSource().getPluginSettings().get(0);
        final Optional<Source> pipelineSource = getSourceIfPipelineType(pipelineName, sourceSetting,
                pipelineMap, pipelineConfigurationMap);
        final Source source = pipelineSource.orElseGet(() -> SourceFactory.newSource(sourceSetting));

        final Buffer buffer = getBufferOrDefault(pipelineConfiguration.getBuffer());

        final Configuration processorConfiguration = pipelineConfiguration.getProcessor();
        final List<PluginSetting> processorPluginSettings = processorConfiguration.getPluginSettings();
        final List<Processor> processors = processorPluginSettings.stream()
                .map(ProcessorFactory::newProcessor)
                .collect(Collectors.toList());
        final int processorThreads = getWorkersOrDefault(processorConfiguration);
        final int readBatchDelay = getDelayOrDefault(processorConfiguration);

        final List<PluginSetting> sinkPluginSettings = pipelineConfiguration.getSink().getPluginSettings();
        final List<Sink> sinks = sinkPluginSettings.stream().map(this::buildSinkOrConnector).collect(Collectors.toList());

        final Pipeline pipeline = new Pipeline(pipelineName, source, buffer, processors, sinks, processorThreads, readBatchDelay);
        pipelineMap.put(pipelineName, pipeline);
    }

    private Buffer getBufferOrDefault(final Configuration bufferConfiguration) {
        final List<PluginSetting> pluginSettings = bufferConfiguration.getPluginSettings();
        return pluginSettings.isEmpty() ? new BlockingBuffer() : BufferFactory.newBuffer(pluginSettings.get(0));
    }

    private int getWorkersOrDefault(final Configuration processorConfiguration) {
        int processorThreads = processorConfiguration.getAttributeValueAsInteger(ATTRIBUTE_WORKERS);
        return processorThreads <= 0 ? getDefaultProcessorThreads() : processorThreads;
    }

    private int getDelayOrDefault(final Configuration processorConfiguration) {
        int readBatchDelay = processorConfiguration.getAttributeValueAsInteger(ATTRIBUTE_DELAY);
        return readBatchDelay <= 0 ? DEFAULT_READ_BATCH_DELAY : readBatchDelay;
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
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            if (!sourceConnectorMap.containsKey(sourcePipelineName)) {
                //Build connected pipeline for the pipeline connector to be available
                buildPipelineFromConfiguration(pipelineNameOptional.get(), pipelineConfigurationMap, pipelineMap);
            }
            return Optional.of(sourceConnectorMap.get(sourcePipelineName));
        }
        return Optional.empty();
    }

    private Sink buildSinkOrConnector(final PluginSetting pluginSetting) {
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