/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RuleEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEvaluator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final TransformersFactory transformersFactory;

    public RuleEvaluator(TransformersFactory transformersFactory) {
        this.transformersFactory = transformersFactory;
    }

    public RuleEvaluatorResult isTransformationNeeded(PipelinesDataFlowModel pipelineModel) {

        Map<String, PipelineModel> pipelines = pipelineModel.getPipelines();
        for (Map.Entry<String, PipelineModel> entry : pipelines.entrySet()) {
            try {
                String pipelineJson = OBJECT_MAPPER.writeValueAsString(entry);
                RuleFileEvaluation ruleFileEvaluation = evaluate(pipelineJson);

                if (ruleFileEvaluation.getResult()) {
                    String pluginName = ruleFileEvaluation.getPluginName();
                    LOG.info("Applying rule {}",ruleFileEvaluation.getRuleFileName().toString());
                    LOG.info("Rule for {} is evaluated true for pipelineJson {}", pluginName, pipelineJson);

                    InputStream templateStream = transformersFactory.getPluginTemplateFileStream(pluginName);
                    PipelineTemplateModel templateModel = yamlMapper.readValue(templateStream,
                            PipelineTemplateModel.class);
                    LOG.info("Template is chosen for {}", pluginName);

                    return RuleEvaluatorResult.builder()
                            .withEvaluatedResult(true)
                            .withPipelineTemplateModel(templateModel)
                            .withPipelineName(entry.getKey())
                            .build();
                }
            } catch (FileNotFoundException e) {
                LOG.error("Template File Not Found");
                throw new RuntimeException(e);
            } catch (JsonProcessingException e) {
                LOG.error("Error processing json");
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOG.error("Error reading file");
                throw new RuntimeException(e);
            }
        }
        return RuleEvaluatorResult.builder()
                .withEvaluatedResult(false)
                .withPipelineName(null)
                .withPipelineTemplateModel(null)
                .build();
    }

    private RuleFileEvaluation evaluate(String pipelinesJson) {
        Configuration parseConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonNodeJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.SUPPRESS_EXCEPTIONS)
                .build();

        RuleTransformerModel rulesModel = null;

        try {
            Collection<RuleStream> ruleStreams = transformersFactory.loadRules();

            //walk through all rules and return first valid
            for (RuleStream ruleStream : ruleStreams) {
                try {
                    rulesModel = yamlMapper.readValue(ruleStream.getRuleStream(), RuleTransformerModel.class);
                    List<String> rules = rulesModel.getApplyWhen();
                    String pluginName = rulesModel.getPluginName();
                    boolean allRulesValid = true;

                    for (String rule : rules) {
                        try {
                            JsonNode result = JsonPath.using(parseConfig).parse(pipelinesJson).read(rule);
                            if (result == null || result.size() == 0) {
                                allRulesValid = false;
                                break;
                            }
                        } catch (PathNotFoundException e) {
                            LOG.debug("Json Path not found for {}", ruleStream.getName());
                            allRulesValid = false;
                            break;
                        }
                    }

                    if (allRulesValid) {
                        return RuleFileEvaluation.builder()
                                .withPluginName(pluginName)
                                .withRuleFileName(ruleStream.getName())
                                .withResult(true)
                                .build();
                    }
                } finally {
                    ruleStream.close();
                }
            }

        } catch (FileNotFoundException e) {
            LOG.debug("Rule File Not Found");
            return RuleFileEvaluation.builder()
                    .withPluginName(null)
                    .withRuleFileName(null)
                    .withResult(false)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return RuleFileEvaluation.builder()
                .withPluginName(null)
                .withRuleFileName(null)
                .withResult(false)
                .build();
    }

}