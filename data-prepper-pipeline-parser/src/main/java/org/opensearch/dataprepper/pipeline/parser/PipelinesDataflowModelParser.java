/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleConfig;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PipelinesDataflowModelParser implements PipelineYamlTransformer{
    private static final Logger LOG = LoggerFactory.getLogger(PipelinesDataflowModelParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

    private final PipelineConfigurationReader pipelineConfigurationReader;

    public PipelinesDataflowModelParser(final PipelineConfigurationReader pipelineConfigurationReader) {
        this.pipelineConfigurationReader = pipelineConfigurationReader;
    }


    //TODO
    // move transformation code after user pipeline validation in pipelineTransformer.java
    public PipelinesDataFlowModel parseConfiguration() {
        final List<PipelinesDataFlowModel> pipelinesDataFlowModels = parseStreamsToPipelinesDataFlowModel();
        PipelinesDataFlowModel pipelinesDataFlowModel = mergePipelinesDataModels(pipelinesDataFlowModels);

        performPipelineConfigurationTransformationIfNeeded(pipelinesDataFlowModel);

        return pipelinesDataFlowModel;
    }

    private void validateDataPrepperVersion(final DataPrepperVersion version) {
        if (Objects.nonNull(version) && !DataPrepperVersion.getCurrentVersion().compatibleWith(version)) {
            LOG.error("The version: {} is not compatible with the current version: {}", version, DataPrepperVersion.getCurrentVersion());
            throw new ParseException(format("The version: %s is not compatible with the current version: %s",
                    version, DataPrepperVersion.getCurrentVersion()));
        }
    }

    private List<PipelinesDataFlowModel> parseStreamsToPipelinesDataFlowModel() {
        return pipelineConfigurationReader.getPipelineConfigurationInputStreams().stream()
                .map(this::parseStreamToPipelineDataFlowModel)
                .collect(Collectors.toList());
    }

    private void performPipelineConfigurationTransformationIfNeeded(PipelinesDataFlowModel pipelinesDataFlowModel){

        //check if transformation is required based on rules present in yaml file
        RuleParser ruleParser = new RuleParser();
        RuleEvaluator ruleEvaluator = new RuleEvaluator();

//        RuleConfig rule = ruleParser.parseRule(templateFileLocation);
        List<RuleConfig> rules = ruleParser.getRules();

        //for all the rules in the rule folder, check if any one of it matches
        // if so, then perform transformation based on template
        for(RuleConfig rule:rules){
            if (ruleEvaluator.isRuleValid(rule, pipelinesDataFlowModel)) {

                String templateFileLocation = ruleEvaluator.getTemplateFileLocationForTransformation(rule);

                //load template dataFlowModel from templateFileLocation.
//                PipelinesDataFlowModel templatePipelineDataModels = getTemplateDataFlowModel(templateFileLocation);

//                TODO
//                validateTemplateModel()

                //transform template dataFlowModel based on pipelineDataFlowModel


                //TODO
//                final List<PipelinesDataFlowModel> pipelineTemplateDataFlowModels = parseStreamsToTemplateDataFlowModel();

                //Transform pipeline configuration
//                final List<PipelinesDataFlowModel> transformedPipelinesDataFlowModels = transformConfiguration(pipelinesDataFlowModels, pipelineTemplateDataFlowModels);

//                return mergePipelinesDataModels(transformedPipelinesDataFlowModels);
            }
        }

    }

//
//    private List<PipelinesDataFlowModel> parseStreamsToTemplateDataFlowModel() {
//
//        return pipelineConfigurationReader.getTemplateInputStreams().stream()
//                .map(this::parseStreamToTemplateDataFlowModel)
//                .collect(Collectors.toList());
//    }

    private PipelinesDataFlowModel parseStreamToPipelineDataFlowModel(final InputStream configurationInputStream) {
        try (final InputStream pipelineConfigurationInputStream = configurationInputStream) {
            final PipelinesDataFlowModel pipelinesDataFlowModel = OBJECT_MAPPER.readValue(pipelineConfigurationInputStream,
                    PipelinesDataFlowModel.class);

            final DataPrepperVersion version = pipelinesDataFlowModel.getDataPrepperVersion();
            validateDataPrepperVersion(version);

            return pipelinesDataFlowModel;
        } catch (IOException e) {
            throw new ParseException("Failed to parse the configuration", e);
        }
    }

    private PipelinesDataFlowModel parseStreamToTemplateDataFlowModel(final InputStream configurationInputStream) {
        try (final InputStream pipelineConfigurationInputStream = configurationInputStream) {
            final PipelinesDataFlowModel pipelinesDataFlowModel = OBJECT_MAPPER.readValue(pipelineConfigurationInputStream,
                    PipelinesDataFlowModel.class);

            return pipelinesDataFlowModel;
        } catch (IOException e) {
            throw new ParseException("Failed to parse the configuration", e);
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
                    "extension/pipeline_configurations and definition must all be defined in a single YAML file if extension/pipeline_configurations is configured.");
        }
        return pipelineExtensionsList.isEmpty() ? new PipelinesDataFlowModel(pipelinesDataFlowModelMap) :
                new PipelinesDataFlowModel(pipelineExtensionsList.get(0), pipelinesDataFlowModelMap);
    }

    @Override
    public String transformYaml(String originalYaml, String templateYaml) {
        return null;
    }

    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel,
                                                         PipelinesDataFlowModel pipelineTemplateDataFlowModel) {

        PipelinesDataFlowModel transformedPipelinesDataFlowModel=null;

        return transformedPipelinesDataFlowModel;
    }
}
