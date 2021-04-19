/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.parser;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.parser.model.PipelineConfiguration;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.PipelineConnector;
import com.amazon.dataprepper.plugins.buffer.BufferFactory;
import com.amazon.dataprepper.plugins.prepper.PrepperFactory;
import com.amazon.dataprepper.plugins.sink.SinkFactory;
import com.amazon.dataprepper.plugins.source.SourceFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
    private final String configurationFileLocation;
    private final Map<String, PipelineConnector> sourceConnectorMap = new HashMap<>(); //TODO Remove this and rely only on pipelineMap

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
            pipelineConfigurationMap.forEach((pipelineName, configuration) ->
                    configuration.updateCommonPipelineConfiguration(pipelineName));
            for (String pipelineName : allPipelineNames) {
                if (!pipelineMap.containsKey(pipelineName) && pipelineConfigurationMap.containsKey(pipelineName)) {
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
            final Buffer buffer = BufferFactory.newBuffer(pipelineConfiguration.getBufferPluginSetting());

            LOG.info("Building preppers for the pipeline [{}]", pipelineName);
            final int prepperThreads = pipelineConfiguration.getWorkers();
            final List<List<Prepper>> prepperSets = pipelineConfiguration.getPrepperPluginSettings().stream()
                    .map(PrepperFactory::newPreppers)
                    .collect(Collectors.toList());
            final int readBatchDelay = pipelineConfiguration.getReadBatchDelay();

            LOG.info("Building sinks for the pipeline [{}]", pipelineName);
            final List<Sink> sinks = pipelineConfiguration.getSinkPluginSettings().stream()
                    .map(this::buildSinkOrConnector)
                    .collect(Collectors.toList());

            final Pipeline pipeline = new Pipeline(pipelineName, source, buffer, prepperSets, sinks, prepperThreads, readBatchDelay);
            pipelineMap.put(pipelineName, pipeline);
        } catch (Exception ex) {
            //If pipeline construction errors out, we will skip that pipeline and proceed
            LOG.error("Construction of pipeline components failed, skipping building of pipeline [{}] and its connected " +
                    "pipelines", pipelineName, ex);
            processRemoveIfRequired(pipelineName, pipelineConfigurationMap, pipelineMap);
        }

    }

    private Optional<Source> getSourceIfPipelineType(
            final String sourcePipelineName,
            final PluginSetting pluginSetting,
            final Map<String, Pipeline> pipelineMap,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        LOG.info("Building [{}] as source component for the pipeline [{}]", pluginSetting.getName(), sourcePipelineName);
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String connectedPipeline = pipelineNameOptional.get();
            if (!sourceConnectorMap.containsKey(sourcePipelineName)) {
                LOG.info("Source of pipeline [{}] requires building of pipeline [{}]", sourcePipelineName,
                        connectedPipeline);
                //Build connected pipeline for the pipeline connector to be available
                //Building like below sometimes yields multiple runs if the pipeline building fails before sink
                //creation. except for running the creation again, it will not harm anything - TODO Fix this
                buildPipelineFromConfiguration(pipelineNameOptional.get(), pipelineConfigurationMap, pipelineMap);
            }
            if (!pipelineMap.containsKey(connectedPipeline)) {
                LOG.error("Connected Pipeline [{}] failed to build, Failing building source for [{}]",
                        connectedPipeline, sourcePipelineName);
                throw new RuntimeException(format("Failed building source for %s, exiting", sourcePipelineName));
            }
            final PipelineConnector pipelineConnector = sourceConnectorMap.get(sourcePipelineName);
            pipelineConnector.setSourcePipelineName(pipelineNameOptional.get());
            return Optional.of(pipelineConnector);
        }
        return Optional.empty();
    }

    private Sink buildSinkOrConnector(final PluginSetting pluginSetting) {
        LOG.info("Building [{}] as sink component", pluginSetting.getName());
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String pipelineName = pipelineNameOptional.get();
            final PipelineConnector pipelineConnector = new PipelineConnector(pipelineName);
            sourceConnectorMap.put(pipelineName, pipelineConnector); //TODO retrieve from parent Pipeline using name
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

    /**
     * This removes all built connected pipelines of given pipeline from pipelineMap.
     * TODO Update this to be more elegant and trigger destroy of plugins
     */
    private void removeConnectedPipelines(
            final String failedPipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration failedPipelineConfiguration = pipelineConfigurationMap.remove(failedPipeline);

        //remove source connected pipelines
        final Optional<String> sourcePipelineOptional = getPipelineNameIfPipelineType(
                failedPipelineConfiguration.getSourcePluginSetting());
        sourcePipelineOptional.ifPresent(sourcePipeline -> processRemoveIfRequired(
                sourcePipeline, pipelineConfigurationMap, pipelineMap));

        //remove sink connected pipelines
        final List<PluginSetting> sinkPluginSettings = failedPipelineConfiguration.getSinkPluginSettings();
        sinkPluginSettings.forEach(sinkPluginSetting -> {
            getPipelineNameIfPipelineType(sinkPluginSetting).ifPresent(sinkPipeline -> processRemoveIfRequired(
                    sinkPipeline, pipelineConfigurationMap, pipelineMap));
        });
    }

    private void processRemoveIfRequired(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        if (pipelineConfigurationMap.containsKey(pipelineName)) {
            pipelineMap.remove(pipelineName);
            sourceConnectorMap.remove(pipelineName);
            removeConnectedPipelines(pipelineName, pipelineConfigurationMap, pipelineMap);
        }
    }
}