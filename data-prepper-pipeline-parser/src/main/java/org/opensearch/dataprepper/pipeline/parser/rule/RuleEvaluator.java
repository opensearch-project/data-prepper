/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.pipeline.parser.rule;

import org.opensearch.dataprepper.model.annotations.TransformationFunction;
import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;

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
import java.util.Arrays;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RuleEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEvaluator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private static final String REQUIRED_PACKAGE_SEGMENT = "dataprepper_transformer";
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
                            .withFunctionProviders(ruleFileEvaluation.getFunctionProviders())
                            .withPipelineName(entry.getKey())
                            .build();
                }
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
                .withFunctionProviders(null)
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

            // Pre-parse all rules and sort by specificity (most conditions first)
            // so more specific rules like rds-joins match before generic rds
            List<ParsedRule> parsedRules = new ArrayList<>();
            for (RuleStream ruleStream : ruleStreams) {
                try {
                    RuleTransformerModel model = yamlMapper.readValue(ruleStream.getRuleStream(), RuleTransformerModel.class);
                    validateFunctionProviders(model.getFunctionProviders(), ruleStream.getName());
                    parsedRules.add(new ParsedRule(model, ruleStream.getName()));
                } finally {
                    ruleStream.close();
                }
            }
            parsedRules.sort((a, b) -> Integer.compare(
                    b.model.getApplyWhen().size(), a.model.getApplyWhen().size()));

            //walk through all rules and return first valid
            for (ParsedRule parsedRule : parsedRules) {
                    rulesModel = parsedRule.model;
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
                            LOG.debug("Json Path not found for {}", parsedRule.fileName);
                            allRulesValid = false;
                            break;
                        }
                    }

                    if (allRulesValid) {
                        return RuleFileEvaluation.builder()
                                .withPluginName(pluginName)
                                .withRuleFileName(parsedRule.fileName)
                                .withFunctionProviders(rulesModel.getFunctionProviders())
                                .withResult(true)
                                .build();
                    }
            }

        } catch (FileNotFoundException e) {
            LOG.debug("Rule File Not Found", e);
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

    private void validateFunctionProviders(List<String> functionProviders, String ruleFileName) {
        if (functionProviders == null || functionProviders.isEmpty()) {
            return;
        }
        for (String provider : functionProviders) {
            if (!provider.contains(REQUIRED_PACKAGE_SEGMENT)) {
                throw new RuntimeException("Invalid function_provider '" + provider +
                        "' in rule file '" + ruleFileName +
                        "'. Package must contain '" + REQUIRED_PACKAGE_SEGMENT + "'");
            }

            final Class<?> clazz;
            try {
                clazz = Class.forName(provider, false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("function_provider class '" + provider +
                        "' in rule file '" + ruleFileName + "' could not be found", e);
            }

            if (!PipelineTransformFunctionProvider.class.isAssignableFrom(clazz)) {
                throw new RuntimeException("function_provider class '" + provider +
                        "' in rule file '" + ruleFileName +
                        "' does not implement PipelineTransformFunctionProvider");
            }

            boolean hasAnnotatedMethod = Arrays.stream(clazz.getMethods())
                    .anyMatch(m -> m.isAnnotationPresent(TransformationFunction.class));
            if (!hasAnnotatedMethod) {
                throw new RuntimeException("function_provider class '" + provider +
                        "' in rule file '" + ruleFileName +
                        "' has no methods annotated with @TransformationFunction");
            }
        }
    }


    private static class ParsedRule {
        final RuleTransformerModel model;
        final String fileName;

        ParsedRule(RuleTransformerModel model, String fileName) {
            this.model = model;
            this.fileName = fileName;
        }
    }
}
